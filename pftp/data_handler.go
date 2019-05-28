package pftp

import (
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net"
	"strconv"
	"strings"
	"time"

	"golang.org/x/sync/errgroup"
)

type dataHandler struct {
	clientConn      connector
	originConn      connector
	config          *config
	log             *logger
	originConnected chan bool
}

type connector struct {
	listener    *net.TCPListener
	conn        net.Conn
	remoteIP    string
	remotePort  string
	localIP     string
	localPort   string
	needsListen bool
	isClient    bool
	mode        string
}

// Make listener for data connection
func newDataHandler(config *config, log *logger, clientConn net.Conn, originConn net.Conn, mode string) (*dataHandler, error) {
	var err error

	d := &dataHandler{
		originConn: connector{
			listener:    nil,
			conn:        nil,
			needsListen: false,
			isClient:    false,
			mode:        config.TransferMode,
		},
		clientConn: connector{
			listener:    nil,
			conn:        nil,
			needsListen: false,
			isClient:    true,
			mode:        mode,
		},
		config:          config,
		log:             log,
		originConnected: make(chan bool),
	}

	if originConn != nil {
		d.originConn.remoteIP, _, _ = net.SplitHostPort(originConn.RemoteAddr().String())
		d.originConn.localIP, d.originConn.localPort, _ = net.SplitHostPort(originConn.LocalAddr().String())
	}

	if clientConn != nil {
		d.clientConn.remoteIP, _, _ = net.SplitHostPort(clientConn.RemoteAddr().String())
		d.clientConn.localIP, d.clientConn.localPort, _ = net.SplitHostPort(clientConn.LocalAddr().String())
	}

	// When connections are nil, will not set listener
	if clientConn == nil || originConn == nil {
		return d, nil
	}

	// init client connection
	if checkNeedListen(d.clientConn.mode, d.originConn.mode, true) {
		d.clientConn.listener, err = d.setNewListener()
		if err != nil {
			connectionCloser(d, d.log)

			return nil, err
		}
		d.clientConn.needsListen = true
	}

	// init origin connection
	if checkNeedListen(d.clientConn.mode, d.originConn.mode, false) {
		d.originConn.listener, err = d.setNewListener()
		if err != nil {
			connectionCloser(d, d.log)

			return nil, err
		}
		d.originConn.needsListen = true
	}

	return d, nil
}

// check needs to open listener
func checkNeedListen(clientMode string, originMode string, isClient bool) bool {
	if isClient {
		if clientMode == "PASV" || clientMode == "EPSV" {
			return true
		}
	} else {
		if originMode == "PORT" || originMode == "EPRT" {
			return true
		} else if originMode == "CLIENT" {
			if clientMode == "PORT" || clientMode == "EPRT" {
				return true
			}
		}
	}

	return false
}

// get listen port
func getListenPort(dataPortRange string) string {
	// random port select
	if len(dataPortRange) == 0 {
		return ""
	}

	portRange := strings.Split(dataPortRange, "-")
	min, _ := strconv.Atoi(strings.TrimSpace(portRange[0]))
	max, _ := strconv.Atoi(strings.TrimSpace(portRange[1]))

	// return min port number
	if min == max {
		return strconv.Itoa(min)
	}

	// random select in min - max range
	return strconv.Itoa(min + rand.Intn(max-min))
}

// assign listen port create listener
func (d *dataHandler) setNewListener() (*net.TCPListener, error) {
	var listener *net.TCPListener
	var lAddr *net.TCPAddr
	var err error
	// reallocate listener port when selected port is busy until LISTEN_TIMEOUT
	counter := 0
	for {
		counter++

		lAddr, err = net.ResolveTCPAddr("tcp", ":"+getListenPort(d.config.DataPortRange))
		if err != nil {
			d.log.err("cannot resolve TCPAddr")
			return nil, err
		}

		// only can support IPv4 between origin server connection
		if listener, err = net.ListenTCP("tcp4", lAddr); err != nil {
			if counter > ConnectionTimeout {
				d.log.err("cannot set listener")

				return nil, err
			}

			d.log.debug("cannot use choosen port. try to select another port after 1 second... (%d/%d)", counter, ConnectionTimeout)

			time.Sleep(time.Duration(1) * time.Second)
			continue

		} else {
			// set listener timeout
			listener.SetDeadline(time.Now().Add(time.Duration(ConnectionTimeout) * time.Second))

			break
		}
	}

	return listener, nil
}

// close all connection and listener
func (d *dataHandler) Close() error {
	lastErr := error(nil)

	// close net.Conn
	if d.clientConn.conn != nil {
		if err := d.clientConn.conn.Close(); err != nil {
			d.log.err("origin data connection close error: %s", err.Error())
			lastErr = err
		}
	}
	if d.originConn.conn != nil {
		if err := d.originConn.conn.Close(); err != nil {
			d.log.err("origin data connection close error: %s", err.Error())
			lastErr = err
		}
	}

	// close listener
	if d.clientConn.listener != nil {
		if err := d.clientConn.listener.Close(); err != nil {
			d.log.err("client data listener close error: %s", err.Error())
			lastErr = err
		}
	}
	if d.originConn.listener != nil {
		if err := d.originConn.conn.Close(); err != nil {
			d.log.err("origin data listener close error: %s", err.Error())
			lastErr = err
		}
	}

	if lastErr == nil {
		d.log.debug("proxy data channel disconnected")
	}

	return lastErr
}

// Make listener for data connection
func (d *dataHandler) StartDataTransfer() error {
	var err error

	eg := errgroup.Group{}

	defer connectionCloser(d, d.log)

	// make data connection
	eg.Go(func() error { return d.clientListenOrDial() })
	eg.Go(func() error { return d.originListenOrDial() })

	// wait until data connection done
	if err = eg.Wait(); err != nil {
		d.log.err(err.Error())

		d.log.debug("data channel creation failed")
		return err
	}

	// client to origin
	eg.Go(func() error { return d.dataTransfer(d.clientConn.conn, d.originConn.conn) })

	// origin to client
	eg.Go(func() error { return d.dataTransfer(d.originConn.conn, d.clientConn.conn) })

	// wait until data transfer goroutine end
	if err = eg.Wait(); err != nil {
		d.log.err(err.Error())
	}

	return nil
}

// make client connection
func (d *dataHandler) clientListenOrDial() error {
	// wait until pftp to origin connected
	if !<-d.originConnected {
		d.log.err("origin connections is failed")
		return fmt.Errorf("origin connections is failed")
	}

	// if client connect needs listen, open listener
	if d.clientConn.needsListen {
		conn, err := d.clientConn.listener.AcceptTCP()
		if err != nil || conn == nil {
			d.log.err("error on client connection listen: %v, %s", conn, err.Error())
			return err
		}

		d.clientConn.listener.Close()
		d.clientConn.listener = nil

		d.log.debug("client connected from %s", conn.RemoteAddr().String())

		// set linger 0 and tcp keepalive setting between client connection
		if d.config.KeepaliveTime > 0 {
			conn.SetKeepAlive(true)
			conn.SetKeepAlivePeriod(time.Duration(d.config.KeepaliveTime) * time.Second)
			conn.SetLinger(0)
		}

		d.clientConn.conn = conn
	} else {
		var conn net.Conn
		var err error

		// try connect second times
		// at first time, connecto by received IP
		// if failed, try connect to communication channel connected IP
		for i := 1; i <= 2; i++ {
			// when connect to client(use active mode), dial to client use port 20 only
			lAddr, err := net.ResolveTCPAddr("tcp", net.JoinHostPort("", "20"))
			if err != nil {
				d.log.err("cannot resolve local address")
				return err
			}
			// set port reuse and local address
			netDialer := net.Dialer{
				Control:   setReuseIPPort,
				LocalAddr: lAddr,
				Deadline:  time.Now().Add(time.Duration(ConnectionTimeout) * time.Second),
			}

			conn, err = netDialer.Dial("tcp", net.JoinHostPort(d.clientConn.remoteIP, d.clientConn.remotePort))
			if err != nil || conn == nil {
				d.log.err("cannot connect to client data address: %v, %s", conn, err.Error())

				// if failed connect to origin by response IP, try to original connection IP again.
				d.clientConn.remoteIP = strings.Split(d.clientConn.conn.RemoteAddr().String(), ":")[0]
				d.log.debug("try to connect original IP: %s", d.clientConn.remoteIP)
			} else {
				break
			}
		}
		if err != nil || conn == nil {
			d.log.err("cannot connect to client data address: %v, %s", conn, err.Error())
			return err
		}

		d.log.debug("connect to client %s", conn.RemoteAddr().String())

		// set linger 0 and tcp keepalive setting between client connection
		tcpConn := conn.(*net.TCPConn)
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(time.Duration(d.config.KeepaliveTime) * time.Second)
		tcpConn.SetLinger(0)

		d.clientConn.conn = tcpConn
	}

	return nil
}

// make origin connection
func (d *dataHandler) originListenOrDial() error {
	// if origin connect needs listen, open listener
	if d.originConn.needsListen {
		conn, err := d.originConn.listener.AcceptTCP()
		if err != nil || conn == nil {
			d.log.err("error on origin connection listen: %v, %s", conn, err.Error())

			d.originConnected <- false

			return err
		}

		d.originConn.listener.Close()
		d.originConn.listener = nil

		d.log.debug("origin connected from %s", conn.RemoteAddr().String())

		// set linger 0 and tcp keepalive setting between client connection
		if d.config.KeepaliveTime > 0 {
			conn.SetKeepAlive(true)
			conn.SetKeepAlivePeriod(time.Duration(d.config.KeepaliveTime) * time.Second)
			conn.SetLinger(0)
		}

		d.originConn.conn = conn

	} else {
		var conn net.Conn
		var err error

		// try connect second times
		// at first time, connecto by received IP
		// if failed, try connect to communication channel connected IP
		for i := 1; i <= 2; i++ {
			conn, err = net.DialTimeout(
				"tcp",
				net.JoinHostPort(d.originConn.remoteIP, d.originConn.remotePort),
				time.Duration(ConnectionTimeout)*time.Second)
			if err != nil || conn == nil {
				d.log.debug("cannot connect to origin data address: %v, %s", conn, err.Error())

				// if failed connect to origin by response IP, try to original connection IP again.
				d.originConn.remoteIP = strings.Split(d.originConn.conn.RemoteAddr().String(), ":")[0]
				d.log.debug("try to connect original IP: %s", d.originConn.remoteIP)
			} else {
				break
			}
		}
		if err != nil || conn == nil {
			d.log.debug("cannot connect to origin data address: %v, %s", conn, err.Error())

			d.originConnected <- false

			return err
		}

		d.log.debug("connected to origin %s", conn.RemoteAddr().String())

		// set linger 0 and tcp keepalive setting between client connection
		tcpConn := conn.(*net.TCPConn)
		tcpConn.SetKeepAlive(true)
		tcpConn.SetKeepAlivePeriod(time.Duration(d.config.KeepaliveTime) * time.Second)
		tcpConn.SetLinger(0)

		// d.originConn.conn = tcpConn
		d.originConn.conn = conn
	}

	d.originConnected <- true

	return nil
}

// send data until got EOF or error on connection
func (d *dataHandler) dataTransfer(reader net.Conn, writer net.Conn) error {
	var err error

	buffer := make([]byte, DataTransferBufferSize)
	if _, err := io.CopyBuffer(writer, reader, buffer); err != nil {
		err = fmt.Errorf("got error on data transfer: %s", err.Error())
	}

	// send EOF to writer. if fail, close connection
	if err := sendEOF(writer); err != nil {
		writer.Close()
	}

	return err
}

// parse port comand line
func (d *dataHandler) parsePORTcommand(line string) error {
	// PORT command format : "PORT h1,h2,h3,h4,p1,p2\r\n"
	var err error

	d.clientConn.remoteIP, d.clientConn.remotePort, err = parseLineToAddr(strings.Split(strings.Trim(line, "\r\n"), " ")[1])

	return err
}

// parse eprt comand line
func (d *dataHandler) parseEPRTcommand(line string) error {
	// EPRT command format
	// - IPv4 : "EPRT |1|h1.h2.h3.h4|port|\r\n"
	// - IPv6 : "EPRT |2|h1::h2:h3:h4:h5|port|\r\n"
	var err error

	d.clientConn.remoteIP, d.clientConn.remotePort, err = parseEPRTtoAddr(strings.Split(strings.Trim(line, "\r\n"), " ")[1])

	return err
}

// parse pasv comand line
func (d *dataHandler) parsePASVresponse(line string) error {
	// PASV response format : "227 Entering Passive Mode (h1,h2,h3,h4,p1,p2).\r\n"
	var err error

	startIndex := strings.Index(line, "(")
	endIndex := strings.LastIndex(line, ")")

	if startIndex == -1 || endIndex == -1 {
		return errors.New("invalid data address")
	}

	d.originConn.remoteIP, d.originConn.remotePort, err = parseLineToAddr(line[startIndex+1 : endIndex])

	return err
}

// parse epsv comand line
func (d *dataHandler) parseEPSVresponse(line string) error {
	// EPSV response format : "229 Entering Extended Passive Mode (|||port|)\r\n"
	startIndex := strings.Index(line, "(")
	endIndex := strings.LastIndex(line, ")")

	if startIndex == -1 || endIndex == -1 {
		return errors.New("invalid data address")
	}

	// get port and verify it
	originPort := strings.Trim(line[startIndex+1:endIndex], "|")
	port, _ := strconv.Atoi(originPort)
	if port <= 0 || port > 65535 {
		return fmt.Errorf("invalid data address")
	}

	d.originConn.remotePort = originPort

	return nil
}

// parse IP and Port from line
func parseLineToAddr(line string) (string, string, error) {
	addr := strings.Split(line, ",")

	if len(addr) != 6 {
		return "", "", fmt.Errorf("invalid data address")
	}

	// Get IP string from line
	ip := strings.Join(addr[0:4], ".")

	// get port number from line
	port1, _ := strconv.Atoi(addr[4])
	port2, _ := strconv.Atoi(addr[5])

	port := (port1 << 8) + port2

	// check IP and Port is valid
	if net.ParseIP(ip) == nil {
		return "", "", fmt.Errorf("invalid data address")
	}

	if port <= 0 || port > 65535 {
		return "", "", fmt.Errorf("invalid data address")
	}

	return ip, strconv.Itoa(port), nil
}

// parse EPRT command from client
func parseEPRTtoAddr(line string) (string, string, error) {
	addr := strings.Split(line, "|")

	if len(addr) != 5 {
		return "", "", fmt.Errorf("invalid data address")
	}

	netProtocol := addr[1]
	IP := addr[2]

	// check port is valid
	port := addr[3]
	if integerPort, err := strconv.Atoi(port); err != nil {
		return "", "", fmt.Errorf("invalid data address")
	} else if integerPort <= 0 || integerPort > 65535 {
		return "", "", fmt.Errorf("invalid data address")
	}

	switch netProtocol {
	case "1", "2":
		// use protocol 1 means IPv4. 2 means IPv6
		// net.ParseIP for validate IP
		if net.ParseIP(IP) == nil {
			return "", "", fmt.Errorf("invalid data address")
		}
		break
	default:
		// wrong network protocol
		return "", "", fmt.Errorf("unknown network protocol")
	}

	return IP, port, nil
}
