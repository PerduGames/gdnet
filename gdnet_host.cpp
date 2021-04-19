#include "gdnet_host.h"

GDNetHost::GDNetHost() :
	_host(NULL),
	_running(false),
	_event_wait(DEFAULT_EVENT_WAIT),
	_max_peers(DEFAULT_MAX_PEERS),
	_max_channels(DEFAULT_MAX_CHANNELS),
	_max_bandwidth_in(0),
	_max_bandwidth_out(0) {
}

void GDNetHost::thread_start() {
	_running = true;
	Thread::Settings s;
	s.priority = (Thread::Priority)PTHREAD_PRIO_INHERIT;
	_thread.start(thread_callback, this, s);
}

void GDNetHost::thread_stop() {
	_running = false;

	_thread.wait_to_finish();
}

void GDNetHost::thread_callback(void *instance) {
	reinterpret_cast<GDNetHost*>(instance)->thread_loop();
}

void GDNetHost::acquireMutex() {
	_accessMutex.lock();
	_hostMutex.lock();
	_accessMutex.unlock();
}

void GDNetHost::releaseMutex() {
	_hostMutex.unlock();
}

int GDNetHost::get_peer_id(PENetPeer* peer) {
	return (int)(peer - _host->peers);
}

void GDNetHost::send_messages() {
	while (!_message_queue.is_empty()) {
		GDNetMessage* message = _message_queue.pop();

		int flags = 0;

		switch (message->get_type()) {
			case GDNetMessage::UNSEQUENCED:
				flags |= PENET_PACKET_FLAG_UNSEQUENCED;
				break;

			case GDNetMessage::RELIABLE:
				flags |= PENET_PACKET_FLAG_RELIABLE;
				break;

			default:
				break;
		}

		PoolByteArray::Read r = message->get_packet().read();
		PENetPacket * penet_packet = penet_packet_create(r.ptr(), message->get_packet().size(), flags);

		if (penet_packet != NULL) {
			if (message->is_broadcast()) {
				penet_host_broadcast(_host, message->get_channel_id(), penet_packet);
			} else {
				penet_peer_send(&_host->peers[message->get_peer_id()], message->get_channel_id(), penet_packet);
			}
		}

		memdelete(message);
	}
}

GDNetEvent* GDNetHost::new_event(const PENetEvent& penet_event) {
	GDNetEvent* event = memnew(GDNetEvent);

	event->set_time(OS::get_singleton()->get_ticks_msec());
	event->set_peer_id(get_peer_id(penet_event.peer));

	switch (penet_event.type) {
		case PENET_EVENT_TYPE_CONNECT: {

			event->set_event_type(GDNetEvent::CONNECT);
			event->set_data(penet_event.data);

		} break;

		case PENET_EVENT_TYPE_RECEIVE: {

			event->set_event_type(GDNetEvent::RECEIVE);
			event->set_channel_id(penet_event.channelID);

			PENetPacket* penet_packet = penet_event.packet;

			PoolByteArray packet;
			packet.resize(penet_packet->dataLength);

			PoolByteArray::Write w = packet.write();
			memcpy(w.ptr(), penet_packet->data, penet_packet->dataLength);

			event->set_packet(packet);

			penet_packet_destroy(penet_packet);

		} break;

		case PENET_EVENT_TYPE_DISCONNECT: {

			event->set_event_type(GDNetEvent::DISCONNECT);
			event->set_data(penet_event.data);

		} break;

		default:
			break;
	}

	return event;
}

void GDNetHost::poll_events() {
	PENetEvent event;

	if (penet_host_service(_host, &event, _event_wait) > 0) {
		_event_queue.push(new_event(event));

		while (penet_host_check_events(_host, &event) > 0) {
			_event_queue.push(new_event(event));
		}
	}
}

void GDNetHost::thread_loop() {
	while (_running) {
		acquireMutex();

		send_messages();
		poll_events();

		releaseMutex();
	}
}

Ref<GDNetPeer> GDNetHost::get_peer(unsigned id) {
	if (_host != NULL && id < _host->peerCount) {
		return memnew(GDNetPeer(this, &_host->peers[id]));
	}

	return Ref<GDNetPeer>(NULL);
}

Error GDNetHost::bind(Ref<GDNetAddress> addr) {
	ERR_FAIL_COND_V(_host != NULL, FAILED);

	if (addr.is_null()) {
		_host = penet_host_create(NULL, _max_peers, _max_channels, _max_bandwidth_in, _max_bandwidth_out);
	} else {
		CharString host_addr = addr->get_host().ascii();

		PENetAddress penet_addr;
		penet_addr.port = addr->get_port();

		if (host_addr.length() == 0) {
			penet_addr.host = PENET_HOST_ANY;
		} else {
			if (penet_address_set_host(&penet_addr, host_addr.get_data()) != 0) {
				ERR_FAIL_COND_V_MSG("penet_address_set_host(...) != 0", FAILED,"Unable to resolve host");
			}
		}

		_host = penet_host_create(&penet_addr, _max_peers, _max_channels, _max_bandwidth_in, _max_bandwidth_out);
	}

	ERR_FAIL_COND_V(_host == NULL, FAILED);

	thread_start();

	return OK;
}

void GDNetHost::unbind() {
	if (_host != NULL) {
		thread_stop();
		penet_host_flush(_host);
		penet_host_destroy(_host);
		_host = NULL;
		_message_queue.clear();
		_event_queue.clear();
	}
}

Ref<GDNetPeer> GDNetHost::host_connect(Ref<GDNetAddress> addr, int data) {
	ERR_FAIL_COND_V(_host == NULL, NULL);

	PENetAddress penet_addr;
	penet_addr.port = addr->get_port();

	CharString host_addr = addr->get_host().ascii();

	if (penet_address_set_host(&penet_addr, host_addr.get_data()) != 0) {
		ERR_FAIL_COND_V_MSG("penet_address_set_host(...) != 0",NULL,"Unable to resolve host");
	}

	PENetPeer* peer = penet_host_connect(_host, &penet_addr, _max_channels, data);

	ERR_FAIL_COND_V(peer == NULL, NULL);

	return memnew(GDNetPeer(this, peer));
}

void GDNetHost::broadcast_packet(const PoolByteArray& packet, int channel_id, int type) {
	ERR_FAIL_COND(_host == NULL);

	GDNetMessage* message = memnew(GDNetMessage((GDNetMessage::Type)type));
	message->set_broadcast(true);
	message->set_channel_id(channel_id);
	message->set_packet(packet);
	_message_queue.push(message);
}

void GDNetHost::broadcast_var(const Variant& var, int channel_id, int type) {
	ERR_FAIL_COND(_host == NULL);

	int len;

	Error err = encode_variant(var, NULL, len);

	ERR_FAIL_COND(err != OK || len == 0);

	GDNetMessage* message = memnew(GDNetMessage((GDNetMessage::Type)type));
	message->set_broadcast(true);
	message->set_channel_id(channel_id);

	PoolByteArray packet;
	packet.resize(len);

	PoolByteArray::Write w = packet.write();
	err = encode_variant(var, &w[0], len);
}

bool GDNetHost::is_event_available() {
	return (!_event_queue.is_empty());
}

int GDNetHost::get_event_count() {
	return (_event_queue.size());
}

Ref<GDNetEvent> GDNetHost::get_event() {
	return (_event_queue.pop());
}

void GDNetHost::_bind_methods() {
	ClassDB::bind_method(D_METHOD("get_peer"),&GDNetHost::get_peer);

	ClassDB::bind_method(D_METHOD("set_event_wait"),&GDNetHost::set_event_wait); // Deprecated
	ClassDB::bind_method(D_METHOD("set_max_peers"),&GDNetHost::set_max_peers);
	ClassDB::bind_method(D_METHOD("set_max_channels"),&GDNetHost::set_max_channels);
	ClassDB::bind_method(D_METHOD("set_max_bandwidth_in"),&GDNetHost::set_max_bandwidth_in);
	ClassDB::bind_method(D_METHOD("set_max_bandwidth_out"),&GDNetHost::set_max_bandwidth_out);

	ClassDB::bind_method(D_METHOD("bind"),&GDNetHost::bind,DEFVAL(NULL));
	ClassDB::bind_method(D_METHOD("unbind"),&GDNetHost::unbind);
	ClassDB::bind_method(D_METHOD("host_connect"),&GDNetHost::host_connect,DEFVAL(0));
	ClassDB::bind_method(D_METHOD("broadcast_packet"),&GDNetHost::broadcast_packet,DEFVAL(0),DEFVAL(GDNetMessage::UNSEQUENCED));
	ClassDB::bind_method(D_METHOD("broadcast_var"),&GDNetHost::broadcast_var,DEFVAL(0),DEFVAL(GDNetMessage::UNSEQUENCED));
	ClassDB::bind_method(D_METHOD("is_event_available"),&GDNetHost::is_event_available);
	ClassDB::bind_method(D_METHOD("get_event_count"),&GDNetHost::get_event_count);
	ClassDB::bind_method(D_METHOD("get_event"),&GDNetHost::get_event);
}
