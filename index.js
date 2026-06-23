const dgram = require("node:dgram");

const IAX_PORT = 4569;
const FRAME_TYPE_IAX = 0x06;
const SUBCLASS_ACK = 0x04;
const SUBCLASS_PONG = 0x03;
const SUBCLASS_POKE = 0x1e;

function buildIaxFullFrame({
	sourceCall,
	destCall,
	timestamp,
	oseqno,
	iseqno,
	subclass,
	retransmit = false,
	cBit = false,
	payload = Buffer.alloc(0),
}) {
	const header = Buffer.alloc(12);
	const firstWord = 0x8000 | (sourceCall & 0x7fff);
	const secondWord = (retransmit ? 0x8000 : 0) | (destCall & 0x7fff);

	header.writeUInt16BE(firstWord, 0);
	header.writeUInt16BE(secondWord, 2);
	header.writeUInt32BE(timestamp >>> 0, 4);
	header.writeUInt8(oseqno & 0xff, 8);
	header.writeUInt8(iseqno & 0xff, 9);
	header.writeUInt8(FRAME_TYPE_IAX, 10);
	header.writeUInt8((cBit ? 0x80 : 0) | (subclass & 0x7f), 11);

	return Buffer.concat([header, payload]);
}

function parseIaxFullFrame(buffer) {
	if (buffer.length < 12) {
		return null;
	}

	const firstWord = buffer.readUInt16BE(0);
	const isFullFrame = (firstWord & 0x8000) !== 0;
	if (!isFullFrame) {
		return null;
	}

	const secondWord = buffer.readUInt16BE(2);
	const sourceCall = firstWord & 0x7fff;
	const destCall = secondWord & 0x7fff;
	const retransmit = (secondWord & 0x8000) !== 0;
	const timestamp = buffer.readUInt32BE(4);
	const oseqno = buffer.readUInt8(8);
	const iseqno = buffer.readUInt8(9);
	const frameType = buffer.readUInt8(10);
	const subclassByte = buffer.readUInt8(11);
	const cBit = (subclassByte & 0x80) !== 0;
	const subclass = subclassByte & 0x7f;
	const payload = buffer.subarray(12);

	return {
		sourceCall,
		destCall,
		retransmit,
		timestamp,
		oseqno,
		iseqno,
		frameType,
		cBit,
		subclass,
		payload,
	};
}

function pokeIaxServer({ host, port = IAX_PORT, timeoutMs = 2000 } = {}) {
	if (typeof host !== "string" || host.length === 0) {
		throw new Error("host is required and must be a non-empty string");
	}

	if (!Number.isFinite(port) || port < 1 || port > 65535) {
		throw new Error("port must be a valid UDP port (1-65535)");
	}

	if (!Number.isFinite(timeoutMs) || timeoutMs <= 0) {
		throw new Error("timeoutMs must be a positive integer in milliseconds");
	}

	return new Promise((resolve, reject) => {
		const socket = dgram.createSocket("udp4");
		const sourceCall = Math.max(1, Math.floor(Math.random() * 0x7fff));
		const sentAt = Date.now();

		// RFC 5456 section 6.7.1: POKE MUST have destination call number 0.
		const pokeFrame = buildIaxFullFrame({
			sourceCall,
			destCall: 0,
			timestamp: 0,
			oseqno: 0,
			iseqno: 0,
			subclass: SUBCLASS_POKE,
		});

		const timeoutHandle = setTimeout(() => {
			socket.close();
			reject(new Error(`Timeout waiting for PONG after ${timeoutMs} ms`));
		}, timeoutMs);

		socket.once("error", (err) => {
			clearTimeout(timeoutHandle);
			socket.close();
			reject(err);
		});

		socket.on("message", (msg, rinfo) => {
			const frame = parseIaxFullFrame(msg);
			if (!frame || frame.frameType !== FRAME_TYPE_IAX) {
				return;
			}

			if (frame.subclass !== SUBCLASS_PONG) {
				return;
			}

			if (frame.destCall !== sourceCall) {
				return;
			}

			// Best-effort ACK for the received PONG.
			const ack = buildIaxFullFrame({
				sourceCall,
				destCall: frame.sourceCall,
				timestamp: frame.timestamp,
				oseqno: 0,
				iseqno: (frame.oseqno + 1) & 0xff,
				subclass: SUBCLASS_ACK,
			});

			socket.send(ack, rinfo.port, rinfo.address, () => {
				clearTimeout(timeoutHandle);
				socket.close();
				resolve({
					remoteAddress: rinfo.address,
					remotePort: rinfo.port,
					remote: `${rinfo.address}:${rinfo.port}`,
					rttMs: Date.now() - sentAt,
					sourceCall,
					remoteCall: frame.sourceCall,
				});
			});
		});

		socket.send(pokeFrame, port, host, (err) => {
			if (err) {
				clearTimeout(timeoutHandle);
				socket.close();
				reject(err);
			}
		});
	});
}

module.exports = pokeIaxServer;