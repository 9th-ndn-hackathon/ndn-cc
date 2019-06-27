import threading
import asyncio
from ndncc.server import Server
from flask import Flask, render_template, request
from flask_socketio import SocketIO
from ndncc.asyncndn import fetch_data_packet, decode_dict, decode_list
from pyndn import Interest, Data
from ndncc.nfd_face_mgmt_pb2 import GeneralStatus, FaceStatusMessage
from pyndn.encoding import ProtobufTlv


app = Flask(__name__,
            static_url_path='',
            static_folder='assets')

app.config['SECRET_KEY'] = '3mlf4j8um6mg2-qlhyzk4ngxxk$8t4hh&$r)%968koxd3i(j#f'
socketio = SocketIO(app)

work_loop = asyncio.new_event_loop()
server = Server(socketio.emit)
thread = threading.Thread(target=server.run_server, args=(work_loop,))
thread.setDaemon(True)
thread.start()


def run_until_complete(event):
    asyncio.set_event_loop(asyncio.new_event_loop())
    return asyncio.get_event_loop().run_until_complete(event)


@app.route('/')
def general_status():
    interest = Interest("/localhost/nfd/status/general")
    interest.mustBeFresh = True
    interest.canBePrefix = True
    ret = run_until_complete(fetch_data_packet(server.face, interest))
    if isinstance(ret, Data):
        name = ret.name.toUri()
        msg = GeneralStatus()
        try:
            ProtobufTlv.decode(msg, ret.content)
        except RuntimeError as exc:
            print("Decoding Error", exc)
            return "NFD is not running"
        status = decode_dict(msg)
        return render_template('general-status.html', name=name, status=status)
    else:
        print("No response")
        return "NFD is not running"


@app.route('/face-list')
def facelist():
    interest = Interest("/localhost/nfd/faces/list")
    interest.mustBeFresh = True
    interest.canBePrefix = True
    ret = run_until_complete(fetch_data_packet(server.face, interest))
    if isinstance(ret, Data):
        name = ret.name.toUri()
        msg = FaceStatusMessage()
        try:
            ProtobufTlv.decode(msg, ret.content)
        except RuntimeError as exc:
            print("Decoding Error", exc)
            return "NFD is not running"
        face_list = decode_list(msg.face_status)
        fields = list(face_list[0].keys())
        return render_template('face-list.html', fields=fields, face_list=face_list)
    else:
        print("No response")
        return "NFD is not running"


@app.route('/face-events')
def index():
    return render_template('faceevents.html')


@app.route('/add-face')
def addface():
    return render_template('add-face.html')


@app.route('/exec/add-face', methods=['POST'])
def exec_addface():
    uri = request.form['ip']
    ret = run_until_complete(server.add_face(uri))
    if ret is None:
        print("No response")
    else:
        print(ret['st_code'], ret['st_text'])
    return render_template('add-face.html', **ret)


@socketio.on('connect')
def face_event_socket():
    print("CONNECTED")


@socketio.on('my event')
def face_event_socket(json):
    print("MY EVENT" + str(json))


if __name__ == '__main__':
    socketio.run(app)
