import sys
import pickle
import logging
from PyQt5 import QtCore, QtWidgets

import zmq
import session

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)


class QtPyRouterSocket(session.PyZmqSocket):

    def send_py_multipart(self, identity, parts, *args, **kwargs):
        return self.sock.send_multipart(
            (identity, self.server_session_id,
             *[pickle.dumps(_) for _ in parts]), *args, **kwargs)

    def recv_py_multipart(self, *args, **kwargs):
        identity, server_session_id, *rest = self.sock.recv_multipart(*args, **kwargs)
        if server_session_id != self.server_session_id:
            log.warning('Recieved a request from client that has a stale server_session_id')
        return (identity, *[pickle.loads(_) for _ in rest])


class QtPyDealerSocket(session.PyZmqSocket):

    def send_py_multipart(self, parts, *args, **kwargs):
        return self.sock.send_multipart(
            (self.server_session_id,
             *[pickle.dumps(_) for _ in parts]), *args, **kwargs)

    def recv_py_multipart(self, *args, **kwargs):
        server_session_id, *rest = self.sock.recv_multipart(*args, **kwargs)
        self.check_server_session_id(server_session_id)
        return [pickle.loads(_) for _ in rest]


class QtPyPubSubSocket(session.PyZmqSocket):

    def send_py_multipart(self, topic, parts, *args, **kwargs):
        return self.sock.send_multipart(
            (topic.encode('ascii'),
             self.server_session_id,
             *[pickle.dumps(_) for _ in parts]), *args, **kwargs)

    def recv_py_multipart(self, *args, **kwargs):
        topic, server_session_id, *rest = self.sock.recv_multipart(*args, **kwargs)
        self.check_server_session_id(server_session_id)
        return (topic.decode(), *[pickle.loads(_) for _ in rest])


class UpdateSubSocket(QtCore.QObject):
    TIMEOUT = 1000
    message = QtCore.pyqtSignal(str)

    def __init__(self, context):
        QtCore.QObject.__init__(self)

        self.context = context
        self.running = False

        # Created thread_local in start()
        self.socket = None

    def start(self):
        # Socket to talk to server
        self.socket = QtPyPubSubSocket(self.context.socket(zmq.SUB))
        self.socket.connect(session.UPDATE_CLIENT_URL)

        # TBD - handle topic subscriptions
        self.socket.subscribe('')

        self.running = True

        self.loop()

    def stop(self):
        log.debug('update socket stop')
        self.running = False
        self.socket.unsubscribe('')
        self.socket.close()

    def loop(self):
        self.message.emit('running ...')
        while self.running:
            try:
                events = self.socket.poll(timeout=self.TIMEOUT)
                if events != 0:  # Not a timeout
                    topic, count = self.socket.recv_py_multipart()
                    if (count % 100000) == 0:
                        self.message.emit(f'{topic}, {count}')
                self.thread().eventDispatcher().processEvents(
                    QtCore.QEventLoop.AllEvents)
            except session.ServerResetException:
                log.warning('reset detected')
            except:
                log.exception('Unhandled exception in loop:', exc_info=True)


class CmdSocket(QtCore.QObject):
    TIMEOUT = 10000

    cmd_response = QtCore.pyqtSignal(str)

    def __init__(self, context):
        QtCore.QObject.__init__(self)

        # Socket to talk to server
        self.context = context

        # Created thread_local in start()
        self.cmd_sock = None
        self.session_id = None

    def start(self):
        self.cmd_sock = QtPyDealerSocket(self.context.socket(zmq.DEALER))
        self.cmd_sock.connect(session.CMD_CLIENT_URL)
        self.session_id = 0

    def stop(self):
        log.debug('cmd socket stop')
        self.cmd_sock.close()

    def cmd(self, cmd):
        """Send a command to the server and wait for the response."""

        self.session_id += 1  # Increment the counter for rpc call

        # Send the request to the server.
        try:
            # This should not block
            # TBD - consider HWM behaviour
            self.cmd_sock.send_py_multipart([self.session_id, cmd])

            while True:

                # Wait for a response to be ready
                events = self.cmd_sock.poll(timeout=self.TIMEOUT)
                if events == 0:
                    log.warning(f"Timeout waiting for a response to cmd: {cmd}")
                    raise TimeoutError
                else:
                    reply_session, reply = self.cmd_sock.recv_py_multipart()
                    if reply_session == self.session_id:
                        self.cmd_response.emit(reply)
                        break
                    log.warning(f'Ignoring stale response: {reply_session} != {self.session_id} ({reply})')
        except session.ServerResetException:
            log.warning('reset detected')
            # self.restart()


class Session(QtCore.QObject):
    do_cmd = QtCore.pyqtSignal(object)
    do_stop = QtCore.pyqtSignal()
    cmd_response = QtCore.pyqtSignal(object)
    msg_received = QtCore.pyqtSignal(object)

    def __init__(self, parent=None):
        QtCore.QObject.__init__(self, parent)

        context = zmq.Context()

        self.update_thread = QtCore.QThread()
        self.update_socket = UpdateSubSocket(context)
        self.update_socket.moveToThread(self.update_thread)
        self.update_thread.started.connect(self.update_socket.start)

        self.cmd_thread = QtCore.QThread()
        self.cmd_socket = CmdSocket(context)
        self.cmd_socket.moveToThread(self.cmd_thread)
        self.cmd_thread.started.connect(self.cmd_socket.start)

        self.update_socket.message.connect(lambda msg: self.msg_received.emit(msg))
        self.cmd_socket.cmd_response.connect(lambda msg: self.cmd_response.emit(msg))

        self.do_cmd.connect(self.cmd_socket.cmd)
        self.do_stop.connect(self.cmd_socket.stop)
        self.do_stop.connect(self.update_socket.stop)

        QtCore.QTimer.singleShot(0, self.update_thread.start)
        QtCore.QTimer.singleShot(0, self.cmd_thread.start)

        self.all_threads = (self.update_thread, self.cmd_thread)

    def cmd(self, cmd):
        self.do_cmd.emit(cmd)

    def stop(self):
        self.do_stop.emit()

    def close(self):
        self.stop()
        # self.update_socket.running = False
        for thread in self.all_threads:
            thread.quit()
        for thread in self.all_threads:
            thread.wait()


class ZeroMQ_Window(QtWidgets.QMainWindow):

    def __init__(self, parent=None):
        QtWidgets.QMainWindow.__init__(self, parent)

        frame = QtWidgets.QFrame()
        label = QtWidgets.QLabel("listening")
        self.button = QtWidgets.QPushButton('cmd')
        self.stop = QtWidgets.QPushButton('stop')
        self.stop.clicked.connect(self.close)

        self.text_edit = QtWidgets.QTextEdit()

        layout = QtWidgets.QVBoxLayout(frame)
        layout.addWidget(label)
        layout.addWidget(self.text_edit)
        layout.addWidget(self.button)
        layout.addWidget(self.stop)

        self.setCentralWidget(frame)

        self.session = Session()
        self.session.msg_received.connect(self.signal_received)
        self.session.cmd_response.connect(self.signal_received)

        self.button.clicked.connect(lambda: self.session.cmd('test_cmd 1'))

    def signal_received(self, message):
        self.text_edit.append("%s\n" % message)

    def closeEvent(self, event):
        self.session.close()

if __name__ == "__main__":
    app = QtWidgets.QApplication(sys.argv)

    mw = ZeroMQ_Window()
    mw.show()

    sys.exit(app.exec_())
