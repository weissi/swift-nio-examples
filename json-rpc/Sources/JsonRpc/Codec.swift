import Foundation
import NIO
import NIOFoundationCompat

private let maxPayload = 1_000_000 // 1MB

// aggregate bytes till delimiter and add delimiter at end
public class NewlineEncoder: MessageToByteEncoder {
    public typealias OutboundIn = ByteBuffer
    public typealias OutboundOut = ByteBuffer

    private let delimiter1: UInt8 = 0x0D // '\r'
    private let delimiter2: UInt8 = 0x0A // '\n'
    private var delimiterBuffer: ByteBuffer?

    // outbound
    public func encode(data: OutboundIn, out: inout ByteBuffer) throws {
        var payload = data
        // original data
        out.writeBuffer(&payload)
        // add delimiter
        out.writeBytes([delimiter1, delimiter2]) // FIXME:
    }
}

// https://www.poplatek.fi/payments/jsonpos/transport
// JSON/RPC messages are framed with the following format (in the following byte-by-byte order):
// 8 bytes: ASCII lowercase hex-encoded length (LEN) of the actual JSON/RPC message (receiver MUST accept both uppercase and lowercase)
// 1 byte: a colon (":", 0x3a), not included in LEN
// LEN bytes: a JSON/RPC message, no leading or trailing whitespace
// 1 byte: a newline (0x0a), not included in LEN
internal final class JsonPosCodec: ByteToMessageDecoder, MessageToByteEncoder {
    public typealias InboundIn = ByteBuffer
    public typealias InboundOut = ByteBuffer
    public typealias OutboundIn = ByteBuffer
    public typealias OutboundOut = ByteBuffer

    private let newline = UInt8(ascii: "\n")
    private let colon = UInt8(ascii: ":")

    public var cumulationBuffer: ByteBuffer?

    private var newlineBuffer: ByteBuffer?
    private var colonBuffer: ByteBuffer?

    public func handlerAdded(context: ChannelHandlerContext) {
        self.newlineBuffer = context.channel.allocator.buffer(capacity: 1)
        self.newlineBuffer!.writeInteger(self.newline)
        self.colonBuffer = context.channel.allocator.buffer(capacity: 1)
        self.colonBuffer!.writeInteger(self.colon)
    }

    // inbound
    public func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        guard buffer.readableBytes < maxPayload else {
            throw CodecError.requestTooLarge
        }
        guard buffer.readableBytes >= 10 else {
            return .needMoreData
        }
        let readableBytesView = buffer.readableBytesView
        // assuming we have the format <length>:<payload>\n
        let lengthView = readableBytesView.prefix(8)       // contains <length>
        let fromColonView = readableBytesView.dropFirst(8) // contains :<payload>\n
        let payloadView = fromColonView.dropFirst()        // contains <payload>\n

        let hex = String(decoding: lengthView, as: Unicode.UTF8.self)
        guard let payloadSize = Int(hex, radix: 16) else {
            throw CodecError.badFraming
        }
        if self.colon != fromColonView.first! {
            throw CodecError.badFraming
        }

        guard payloadView.count >= payloadSize + 1 && self.newline == payloadView.last else {
            return .needMoreData
        }

        // slice the buffer
        assert(payloadView.startIndex == readableBytesView.startIndex + 9)
        let slice = buffer.getSlice(at: payloadView.startIndex, length: payloadSize)!
        buffer.moveReaderIndex(to: payloadSize + 10)
        // call next handler
        context.fireChannelRead(wrapInboundOut(slice))
        return .continue
    }

    public func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState {
        while try self.decode(context: context, buffer: &buffer) == .continue {}
        if buffer.readableBytes > 0 {
            throw CodecError.badFraming
        }
        return .needMoreData
    }

    // outbound
    public func encode(data: OutboundIn, out: inout ByteBuffer) throws {
        var payload = data
        // length
        out.writeString(String(payload.readableBytes, radix: 16).leftPadding(toLength: 8, withPad: "0"))
        // colon
        out.writeBytes([colon]) // FIXME:
        // payload
        out.writeBuffer(&payload)
        // newline
        out.writeBytes([newline]) // FIXME:
    }
}

// no delimeter is provided, brute force try to decode the json
internal final class BruteForceCodec<T>: ByteToMessageDecoder, MessageToByteEncoder where T: Decodable {
    public typealias InboundIn = ByteBuffer
    public typealias InboundOut = ByteBuffer
    public typealias OutboundIn = ByteBuffer
    public typealias OutboundOut = ByteBuffer

    private let last: UInt8 = 0x7D // '}'

    public func decode(context: ChannelHandlerContext, buffer: inout ByteBuffer) throws -> DecodingState {
        let readable: Int? = try buffer.withUnsafeReadableBytes { bytes in
            if bytes.count >= maxPayload {
                throw CodecError.requestTooLarge
            }
            if last != bytes[bytes.count - 1] {
                return nil
            }
            let data = buffer.getData(at: buffer.readerIndex, length: bytes.count)!
            do {
                _ = try JSONDecoder().decode(T.self, from: data)
                return bytes.count
            } catch is DecodingError {
                return nil
            }
        }
        guard let length = readable else {
            return .needMoreData
        }
        // slice the buffer
        let slice = buffer.readSlice(length: length)!
        // call next handler
        context.fireChannelRead(wrapInboundOut(slice))
        return .continue
    }

    public func decodeLast(context: ChannelHandlerContext, buffer: inout ByteBuffer, seenEOF: Bool) throws -> DecodingState {
        while try self.decode(context: context, buffer: &buffer) == .continue {}
        if buffer.readableBytes > 0 {
            throw CodecError.badFraming
        }
        return .needMoreData
    }

    // FIXME: seems redundant
    // outbound
    public func encode(data: OutboundIn, out: inout ByteBuffer) throws {
        var payload = data
        out.writeBuffer(&payload)
    }
}

// bytes to codable and back
internal final class CodableCodec<In, Out>: ChannelInboundHandler, ChannelOutboundHandler where In: Decodable, Out: Encodable {
    public typealias InboundIn = ByteBuffer
    public typealias InboundOut = In
    public typealias OutboundIn = Out
    public typealias OutboundOut = ByteBuffer

    private let decoder = JSONDecoder()
    private let encoder = JSONEncoder()

    // inbound
    public func channelRead(context: ChannelHandlerContext, data: NIOAny) {
        var buffer = unwrapInboundIn(data)
        let data = buffer.readData(length: buffer.readableBytes)!
        do {
            print("--> decoding \(String(decoding: data[data.startIndex ..< min(data.startIndex + 100, data.endIndex)], as: UTF8.self))")
            let decodable = try self.decoder.decode(In.self, from: data)
            // call next handler
            context.fireChannelRead(wrapInboundOut(decodable))
        } catch let error as DecodingError {
            context.fireErrorCaught(CodecError.badJson(error))
        } catch {
            context.fireErrorCaught(error)
        }
    }

    // outbound
    public func write(context: ChannelHandlerContext, data: NIOAny, promise: EventLoopPromise<Void>?) {
        do {
            let encodable = self.unwrapOutboundIn(data)
            let data = try encoder.encode(encodable)
            print("<-- encoding \(String(decoding: data, as: UTF8.self))")
            var buffer = context.channel.allocator.buffer(capacity: data.count)
            buffer.writeBytes(data)
            context.write(wrapOutboundOut(buffer), promise: promise)
        } catch let error as EncodingError {
            promise?.fail(CodecError.badJson(error))
        } catch {
            promise?.fail(error)
        }
    }
}

internal enum CodecError: Error {
    case badFraming
    case badJson(Error)
    case requestTooLarge
}
