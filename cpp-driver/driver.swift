//
//  driver.swift
//  cpp-driver
//
//  Created by Philippe on 21/10/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

// http://docs.datastax.com/en/developer/cpp-driver/2.7/

import Foundation

public
typealias Checker_f = (Error?) -> Bool
public
protocol Error {
    func error() -> String?
    func check(checker: Checker_f) -> Bool
}
func checkError(_ err_: Error?) -> Bool {
    if let err = err_?.error() {
        print(err)
        fatalError(err)
    }
    return true
}

public
class UuidGenerator {
    let uuid_gen = cass_uuid_gen_new()
    deinit {
        cass_uuid_gen_free(uuid_gen)
    }
    public func time_uuid() -> UUID {
        var cass_uuid = CassUuid(time_and_version: 0,clock_seq_and_node: 0)
        cass_uuid_gen_time(uuid_gen, &cass_uuid)
        let u = uuid(cass_uuid: &cass_uuid)
        return u
    }
    public func timestamp(_ u: UUID) -> Date {
        let cass_uuid = uuid(uuid: u)
        let date = Date(timeIntervalSince1970: TimeInterval(cass_uuid_timestamp(cass_uuid)) / 1000)
        return date
    }
}

func utf8_string(data: UnsafePointer<Int8>?, len: Int) -> String? {
    if nil == data || 0 > len {
        return nil
    }
    let p = UnsafeMutablePointer<Int8>.allocate(capacity: len+1)
    defer {
        p.deallocate(capacity: len+1)
    }
    p.initialize(to: 0, count:len+1)
    strncpy(p, data, len)
    return String(validatingUTF8: p)
}

func error_string(_ future: OpaquePointer!) -> String? {
    var message: UnsafePointer<Int8>?
    var message_length: Int = 0
    cass_future_error_message(future, &message, &message_length)
    return utf8_string(data: message, len: message_length)
}

func uuid(cass_uuid: inout CassUuid) -> UUID {
    let bytesPointer = UnsafeMutableRawPointer.allocate(bytes: 16, alignedTo: 1)
    defer {
        bytesPointer.deallocate(bytes: 16, alignedTo: 1)
    }
    bytesPointer.copyBytes(from: &cass_uuid, count: 16)
    let pu = bytesPointer.bindMemory(to: UInt8.self, capacity: 16)
    let u = UUID(uuid: uuid_t(
        (pu+3).pointee,
        (pu+2).pointee,
        (pu+1).pointee,
        (pu+0).pointee,
        (pu+5).pointee,
        (pu+4).pointee,
        (pu+7).pointee,
        (pu+6).pointee,
        (pu+15).pointee,
        (pu+14).pointee,
        (pu+13).pointee,
        (pu+12).pointee,
        (pu+11).pointee,
        (pu+10).pointee,
        (pu+9).pointee,
        (pu+8).pointee)
    )
    return u
}
func uuid(uuid: UUID) -> CassUuid {
    let a = [uuid.uuid.3,
             uuid.uuid.2,
             uuid.uuid.1,
             uuid.uuid.0,
             uuid.uuid.5,
             uuid.uuid.4,
             uuid.uuid.7,
             uuid.uuid.6,
             uuid.uuid.15,
             uuid.uuid.14,
             uuid.uuid.13,
             uuid.uuid.12,
             uuid.uuid.11,
             uuid.uuid.10,
             uuid.uuid.9,
             uuid.uuid.8]
    let bytesPointer = UnsafeMutableRawPointer.allocate(bytes: 16, alignedTo: 8)
    defer {
        bytesPointer.deallocate(bytes: 16, alignedTo: 8)
    }
    bytesPointer.copyBytes(from: a, count: 16)
    let pu = bytesPointer.bindMemory(to: CassUuid.self, capacity: 1)
    return pu.pointee
}
fileprivate func string(uuid: uuid_t, upper: Bool = false) -> String {
    let fmt = upper
        ? "%02X%02X%02X%02X-%02X%02X-%02X%02X-%02X%02X-%02X%02X%02X%02X%02X%02X"
        : "%02x%02x%02x%02x-%02x%02x-%02x%02x-%02x%02x-%02x%02x%02x%02x%02x%02x"
    return String(format: fmt,
                  uuid.0,
                  uuid.1,
                  uuid.2,
                  uuid.3,

                  uuid.4,
                  uuid.5,

                  uuid.6,
                  uuid.7,

                  uuid.8,
                  uuid.9,

                  uuid.10,
                  uuid.11,
                  uuid.12,
                  uuid.13,
                  uuid.14,
                  uuid.15)
}

/*
 CASS_EXPORT CassFuture* cass_session_connect(CassSession* session, const CassCluster* cluster);
 CASS_EXPORT CassFuture* cass_session_connect_keyspace(CassSession* session, const CassCluster* cluster, const char* keyspace);
 CASS_EXPORT CassFuture* cass_session_close(CassSession* session);
 CASS_EXPORT CassFuture* cass_session_prepare(CassSession* session, const char* query);
 CASS_EXPORT CassFuture* cass_session_execute(CassSession* session, const CassStatement* statement);
 CASS_EXPORT CassFuture* cass_session_execute_batch(CassSession* session, const CassBatch* batch);
 */
/*
 null
 int8
 int16
 int32
 uint32
 int64
 float
 double
 bool
 string
 bytes  (bytes value, int size)
 uuid   (CassUuid)
 inet   (CassInet)
 decimal    (bytes varint, int size, int32 scale)
 duration (int32 months, int32 days, int64 nanos)
 collection (list, set, map)    (CassCollection)
 tuple  (CassTuple)
 user_type  (CassUserType)

 custom?    (string class_name, bytes value, size int)

 */
/*
 cass_statement_bind_null(CassStatement* statement          => nil
 cass_statement_bind_int8(CassStatement* statement          => Int8
 cass_statement_bind_int16(CassStatement* statement         => Int16
 cass_statement_bind_int32(CassStatement* statement         => Int32
 cass_statement_bind_uint32(CassStatement* statement        => UInt32
 cass_statement_bind_int64(CassStatement* statement         => Int64, Foundation.Date
 cass_statement_bind_float(CassStatement* statement         => Float
 cass_statement_bind_double(CassStatement* statement        => Double
 cass_statement_bind_bool(CassStatement* statement          => Bool
 cass_statement_bind_string(CassStatement* statement        => String
 cass_statement_bind_bytes(CassStatement* statement         => Array<UInt8>
 cass_statement_bind_custom(CassStatement* statement
 cass_statement_bind_uuid(CassStatement* statement          => Foundation.UUID
 cass_statement_bind_inet(CassStatement* statement
 cass_statement_bind_decimal(CassStatement* statement          => Foundation.Decimal?
 cass_statement_bind_duration(CassStatement* statement        => Foundation.?
 cass_statement_bind_collection(CassStatement* statement    => Set, Array, Dictionary
 cass_statement_bind_tuple(CassStatement* statement             => Foundation.?
 */
func bind(_ statement: OpaquePointer, lst: [Any?]) {
    for (idx,value) in lst.enumerated() {
        if let v = value {
            let t = type(of:v)
            print("index=",idx,"type of=",t,"->", type(of:t))
        }
        switch value {
        case nil:
            print(idx,"<nil>")
            cass_statement_bind_null(statement, idx)

        case let v as String:
            print(idx,"String",v)
            cass_statement_bind_string(statement, idx,v)
        case let v as Bool:
            print(idx,"Bool",v)
            cass_statement_bind_bool(statement, idx, (v ? cass_true : cass_false))
        case let v as Float32/*, case let v as Float*/:
            print(idx,"Float32 (float)",v)
            cass_statement_bind_float(statement, idx, v)
        case let v as Float64/*, let v as Double*/:
            print(idx,"Float64 (double)",v)
            cass_statement_bind_double(statement, idx, v)
        case let v as Int8 /*, let v as Int*/:
            print(idx,"Int8",v)
            cass_statement_bind_int8(statement, idx, v)
        case let v as Int16 /*, let v as Int*/:
            print(idx,"Int16",v)
            cass_statement_bind_int16(statement, idx, v)
        case let v as Int32 /*, let v as Int*/:
            print(idx,"Int32",v)
            cass_statement_bind_int32(statement, idx, v)
        case let v as Int64 /*, let v as Int*/:
            print(idx,"Int64",v)
            cass_statement_bind_int64(statement, idx, v)
        case let v as Array<UInt8>:
            print(idx,"Array<UInt8>",v)
            cass_statement_bind_bytes(statement, idx, v, v.count)
        // Foundation types
        case let v as UUID:
            print(idx,"UUID",v)
            cass_statement_bind_uuid(statement, idx, uuid(uuid:v))
        case let v as Date:
            print(idx,"Date",v)
            cass_statement_bind_int64(statement, idx, Int64(v.timeIntervalSince1970 * 1000))
            /*case let v as Decimal:
             print(idx,"Decimal",v)
             let exp = Int32(v.exponent)
             let u = NSDecimalNumber(decimal: v.significand).int64Value
             print(">>> u=\(u) exp=\(exp) \(String(format:"%02X",u))")
             var ptr = UnsafeMutableRawPointer.allocate(bytes: 8, alignedTo: 8)
             defer {
             ptr.deallocate(bytes: 8, alignedTo: 8)
             }
             ptr.storeBytes(of: u, as: Int64.self)
             let ia = Array(UnsafeBufferPointer(start: ptr.bindMemory(to: UInt8.self, capacity: 8), count: 8))
             var n = 0
             for b in ia {
             n += 1
             if 0 == b || 255 == b {
             break
             }
             }
             let dec = ia[0..<n]
             print(">>> u=\(u) exp=\(exp) ptr=\(ptr) n=\(n) dec=\(dec) \(type(of: dec))")
             let rdec = Array(dec.reversed())
             print(">>> u=\(u) exp=\(exp) ptr=\(ptr) dec=\(rdec) \(type(of: rdec))")
             let val = UnsafeRawPointer(rdec).bindMemory(to: UInt8.self, capacity: n)
             cass_statement_bind_decimal(statement, idx, val, n, -exp)*/

        case let vs as Set<String>:
            print(idx,"Set<String>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_string(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Set<Bool>:
            print(idx,"Set<Bool>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_bool(collection, v ? cass_true : cass_false)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Set<Float32>/*, let vs as Set<Float>*/:
            print(idx,"Set<Float32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_float(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Set<Float64>/*, let vs as Set<Double>*/:
            print(idx,"Set<Float64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_double(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Set<Int8> /*, let vs as Set<Int>*/:
            print(idx,"Set<Int8>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_int8(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Set<Int16> /*, let vs as Set<Int>*/:
            print(idx,"Set<Int16>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_int16(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Set<Int32> /*, let vs as Set<Int>*/:
            print(idx,"Set<Int32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_int32(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Set<Int64> /*, let vs as Set<Int>*/:
            print(idx,"Set<Int64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_int64(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)

        case let vs as Array<String>:
            print(idx,"Array<String>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_string(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Array<Bool>:
            print(idx,"Array<Bool>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_bool(collection, v ? cass_true : cass_false)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Array<Float32>/*, let v as Array<Float>*/:
            print(idx,"Array<Float32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_float(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Array<Float64>/*, let vs as Array<Double>*/:
            print(idx,"Array<Float64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_double(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Array<Int8> /*, let vs as Array<Int>*/:
            print(idx,"Array<Int8>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_int8(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Array<Int16> /*, let vs as Array<Int>*/:
            print(idx,"Array<Int16>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_int16(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Array<Int32> /*, let vs as Array<Int>*/:
            print(idx,"Array<Int32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_int32(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Array<Int64> /*, let vs as Array<Int>*/:
            print(idx,"Array<Int64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_int64(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)

        case let vs as Dictionary<String, String>:
            print(idx,"Dictionary<String, String>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_string(collection, k)
                cass_collection_append_string(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<String, Bool>:
            print(idx,"Dictionary<String, Bool>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_string(collection, k)
                cass_collection_append_bool(collection, v ? cass_true : cass_false)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<String, Float32>:
            print(idx,"Dictionary<String, Float32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_string(collection, k)
                cass_collection_append_float(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<String, Float64>:
            print(idx,"Dictionary<String, Float64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_string(collection, k)
                cass_collection_append_double(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<String, Int8>:
            print(idx,"Dictionary<String, Int8>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_string(collection, k)
                cass_collection_append_int8(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<String, Int16>:
            print(idx,"Dictionary<String, Int16>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_string(collection, k)
                cass_collection_append_int16(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<String, Int32>:
            print(idx,"Dictionary<String, Int32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_string(collection, k)
                cass_collection_append_int32(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<String, Int64>:
            print(idx,"Dictionary<String, Int64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_string(collection, k)
                cass_collection_append_int64(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Bool, String>:
            print(idx,"Dictionary<Bool, String>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_bool(collection, k ? cass_true : cass_false)
                cass_collection_append_string(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Bool, Bool>:
            print(idx,"Dictionary<Bool, Bool>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_bool(collection, k ? cass_true : cass_false)
                cass_collection_append_bool(collection, v ? cass_true : cass_false)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Bool, Float32>:
            print(idx,"Dictionary<Bool, Float32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_bool(collection, k ? cass_true : cass_false)
                cass_collection_append_float(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Bool, Float64>:
            print(idx,"Dictionary<Bool, Float64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_bool(collection, k ? cass_true : cass_false)
                cass_collection_append_double(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Bool, Int8>:
            print(idx,"Dictionary<Bool, Int8>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_bool(collection, k ? cass_true : cass_false)
                cass_collection_append_int8(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Bool, Int16>:
            print(idx,"Dictionary<Bool, Int16>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_bool(collection, k ? cass_true : cass_false)
                cass_collection_append_int16(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Bool, Int32>:
            print(idx,"Dictionary<Bool, Int32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_bool(collection, k ? cass_true : cass_false)
                cass_collection_append_int32(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Bool, Int64>:
            print(idx,"Dictionary<Bool, Int64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_bool(collection, k ? cass_true : cass_false)
                cass_collection_append_int64(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Float32, String>:
            print(idx,"Dictionary<Float32, String>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_float(collection, k)
                cass_collection_append_string(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Float32, Bool>:
            print(idx,"Dictionary<Float32, Bool>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_float(collection, k)
                cass_collection_append_bool(collection, v ? cass_true : cass_false)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Float32, Float32>:
            print(idx,"Dictionary<Float32, Float32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_float(collection, k)
                cass_collection_append_float(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Float32, Float64>:
            print(idx,"Dictionary<Float32, Float64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_float(collection, k)
                cass_collection_append_double(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Float32, Int8>:
            print(idx,"Dictionary<Float32, Int8>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_float(collection, k)
                cass_collection_append_int8(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Float32, Int16>:
            print(idx,"Dictionary<Float32, Int16>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_float(collection, k)
                cass_collection_append_int16(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Float32, Int32>:
            print(idx,"Dictionary<Float32, Int32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_float(collection, k)
                cass_collection_append_int32(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Float32, Int64>:
            print(idx,"Dictionary<Float32, Int64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_float(collection, k)
                cass_collection_append_int64(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Float64, String>:
            print(idx,"Dictionary<Float64, String>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_double(collection, k)
                cass_collection_append_string(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Float64, Bool>:
            print(idx,"Dictionary<Float64, Bool>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_double(collection, k)
                cass_collection_append_bool(collection, v ? cass_true : cass_false)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Float64, Float32>:
            print(idx,"Dictionary<Float64, Float32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_double(collection, k)
                cass_collection_append_float(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Float64, Float64>:
            print(idx,"Dictionary<Float64, Float64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_double(collection, k)
                cass_collection_append_double(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Float64, Int8>:
            print(idx,"Dictionary<Float64, Int8>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_double(collection, k)
                cass_collection_append_int8(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Float64, Int16>:
            print(idx,"Dictionary<Float64, Int16>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_double(collection, k)
                cass_collection_append_int16(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Float64, Int32>:
            print(idx,"Dictionary<Float64, Int32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_double(collection, k)
                cass_collection_append_int32(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Float64, Int64>:
            print(idx,"Dictionary<Float64, Int64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_double(collection, k)
                cass_collection_append_int64(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int8, String>:
            print(idx,"Dictionary<Int8, String>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int8(collection, k)
                cass_collection_append_string(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int8, Bool>:
            print(idx,"Dictionary<Int8, Bool>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int8(collection, k)
                cass_collection_append_bool(collection, v ? cass_true : cass_false)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int8, Float32>:
            print(idx,"Dictionary<Int8, Float32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int8(collection, k)
                cass_collection_append_float(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int8, Float64>:
            print(idx,"Dictionary<Int8, Float64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int8(collection, k)
                cass_collection_append_double(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int8, Int8>:
            print(idx,"Dictionary<Int8, Int8>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int8(collection, k)
                cass_collection_append_int8(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int8, Int16>:
            print(idx,"Dictionary<Int8, Int16>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int8(collection, k)
                cass_collection_append_int16(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int8, Int32>:
            print(idx,"Dictionary<Int8, Int32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int8(collection, k)
                cass_collection_append_int32(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int8, Int64>:
            print(idx,"Dictionary<Int8, Int64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int8(collection, k)
                cass_collection_append_int64(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int16, String>:
            print(idx,"Dictionary<Int16, String>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int16(collection, k)
                cass_collection_append_string(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int16, Bool>:
            print(idx,"Dictionary<Int16, Bool>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int16(collection, k)
                cass_collection_append_bool(collection, v ? cass_true : cass_false)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int16, Float32>:
            print(idx,"Dictionary<Int16, Float32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int16(collection, k)
                cass_collection_append_float(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int16, Float64>:
            print(idx,"Dictionary<Int16, Float64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int16(collection, k)
                cass_collection_append_double(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int16, Int8>:
            print(idx,"Dictionary<Int16, Int8>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int16(collection, k)
                cass_collection_append_int8(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int16, Int16>:
            print(idx,"Dictionary<Int16, Int16>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int16(collection, k)
                cass_collection_append_int16(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int16, Int32>:
            print(idx,"Dictionary<Int16, Int32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int16(collection, k)
                cass_collection_append_int32(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int16, Int64>:
            print(idx,"Dictionary<Int16, Int64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int16(collection, k)
                cass_collection_append_int64(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int32, String>:
            print(idx,"Dictionary<Int32, String>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int32(collection, k)
                cass_collection_append_string(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int32, Bool>:
            print(idx,"Dictionary<Int32, Bool>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int32(collection, k)
                cass_collection_append_bool(collection, v ? cass_true : cass_false)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int32, Float32>:
            print(idx,"Dictionary<Int32, Float32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int32(collection, k)
                cass_collection_append_float(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int32, Float64>:
            print(idx,"Dictionary<Int32, Float64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int32(collection, k)
                cass_collection_append_double(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int32, Int8>:
            print(idx,"Dictionary<Int32, Int8>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int32(collection, k)
                cass_collection_append_int8(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int32, Int16>:
            print(idx,"Dictionary<Int32, Int16>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int32(collection, k)
                cass_collection_append_int16(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int32, Int32>:
            print(idx,"Dictionary<Int32, Int32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int32(collection, k)
                cass_collection_append_int32(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int32, Int64>:
            print(idx,"Dictionary<Int32, Int64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int32(collection, k)
                cass_collection_append_int64(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int64, String>:
            print(idx,"Dictionary<Int64, String>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int64(collection, k)
                cass_collection_append_string(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int64, Bool>:
            print(idx,"Dictionary<Int64, Bool>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int64(collection, k)
                cass_collection_append_bool(collection, v ? cass_true : cass_false)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int64, Float32>:
            print(idx,"Dictionary<Int64, Float32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int64(collection, k)
                cass_collection_append_float(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int64, Float64>:
            print(idx,"Dictionary<Int64, Float64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int64(collection, k)
                cass_collection_append_double(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int64, Int8>:
            print(idx,"Dictionary<Int64, Int8>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int64(collection, k)
                cass_collection_append_int8(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int64, Int16>:
            print(idx,"Dictionary<Int64, Int16>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int64(collection, k)
                cass_collection_append_int16(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int64, Int32>:
            print(idx,"Dictionary<Int64, Int32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int64(collection, k)
                cass_collection_append_int32(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)
        case let vs as Dictionary<Int64, Int64>:
            print(idx,"Dictionary<Int64, Int64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int64(collection, k)
                cass_collection_append_int64(collection, v)
            }
            cass_statement_bind_collection(statement, idx, collection)

        default:
            print("*** index=\(idx), type of=\(type(of:value!)), Any=\(value!)")
            fatalError("Invalid argument: index=\(idx), type of=\(type(of:value!)), Any=\(value!)")
        }
    }
}
func bind(_ statement: OpaquePointer, map: [String: Any?]) {
    /*print("string",type(of:String.self))
     print("int",type(of:Int.self))
     let dico = [AnyHashable : Any?]()
     print("dico",dico)*/
    for (nam, value) in map {
        let f = {(_ value: Any?) -> () in
            if let v = value {
                let t = type(of:v)
                print("name=",nam," type of=",t,"->", type(of:t))
            }
        }
        f(value)
        switch value {
        case nil:
            print(nam,"<nil>")
            cass_statement_bind_null_by_name(statement, nam)

        case let v as String:
            print(nam,"String",v)
            cass_statement_bind_string_by_name(statement, nam,v)
        case let v as Bool:
            print(nam,"Bool",v)
            cass_statement_bind_bool_by_name(statement, nam, (v ? cass_true : cass_false))
        case let v as Float32/*, let v as Float*/:
            print(nam,"Float32 (float)",v)
            cass_statement_bind_float_by_name(statement, nam, v)
        case let v as Float64/*, let v as Double*/:
            print(nam,"Float64 (double)",v)
            cass_statement_bind_double_by_name(statement, nam, v)
        case let v as Int8 /*, let v as Int*/:
            print(nam,"Int8",v)
            cass_statement_bind_int8_by_name(statement, nam, v)
        case let v as Int16 /*, let v as Int*/:
            print(nam,"Int16",v)
            cass_statement_bind_int16_by_name(statement, nam, v)
        case let v as Int32 /*, let v as Int*/:
            print(nam,"Int32",v)
            cass_statement_bind_int32_by_name(statement, nam, v)
        case let v as Int64 /*, let v as Int*/:
            print(nam,"Int64",v)
            cass_statement_bind_int64_by_name(statement, nam, v)
        case let v as Array<UInt8>:
            print(nam,"Array<UInt8>",v)
            cass_statement_bind_bytes_by_name(statement, nam, v, v.count)
        // Foundation
        case let v as UUID:
            print(nam,"uuid_t",v)
            cass_statement_bind_uuid_by_name(statement, nam, uuid(uuid:v))
        case let v as Date:
            print(nam,"Date",v)
            cass_statement_bind_int64_by_name(statement, nam, Int64(v.timeIntervalSince1970 * 1000))
            /*case let v as Decimal:
             print(nam,"Decimal",v)
             let exp = Int32(v.exponent)
             let u = NSDecimalNumber(decimal: v.significand).int64Value
             print(">>> u=\(u) exp=\(exp) \(String(format:"%02X",u))")
             var ptr = UnsafeMutableRawPointer.allocate(bytes: 8, alignedTo: 8)
             defer {
             ptr.deallocate(bytes: 8, alignedTo: 8)
             }
             ptr.storeBytes(of: u, as: Int64.self)
             let ia = Array(UnsafeBufferPointer(start: ptr.bindMemory(to: UInt8.self, capacity: 8), count: 8))
             var n = 0
             for b in ia {
             n += 1
             if 0 == b || 255 == b {
             break
             }
             }
             let dec = ia[0..<n]
             print(">>> u=\(u) exp=\(exp) ptr=\(ptr) n=\(n) dec=\(dec) \(type(of: dec))")
             let rdec = Array(dec.reversed())
             print(">>> u=\(u) exp=\(exp) ptr=\(ptr) dec=\(rdec) \(type(of: rdec))")
             let val = UnsafeRawPointer(rdec).bindMemory(to: UInt8.self, capacity: n)
             cass_statement_bind_decimal_by_name(statement, nam, val, n, -exp)*/

        case let vs as Set<String>:
            print(nam,"Set<String>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_string(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Set<Bool>:
            print(nam,"Set<Bool>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_bool(collection, v ? cass_true : cass_false)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Set<Float32>/*, let vs as Set<Float>*/:
            print(nam,"Set<Float32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_float(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Set<Float64>/*, let vs as Set<Double>*/:
            print(nam,"Set<Float64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_double(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Set<Int8> /*, let vs as Set<Int>*/:
            print(nam,"Set<Int8>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_int8(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Set<Int16> /*, let vs as Set<Int>*/:
            print(nam,"Set<Int16>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_int16(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Set<Int32> /*, let vs as Set<Int>*/:
            print(nam,"Set<Int32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_int32(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Set<Int64> /*, let vs as Set<Int>*/:
            print(nam,"Set<Int64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_SET, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_int64(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)

        case let vs as Array<String>:
            print(nam,"Array<String>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_string(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Array<Bool>:
            print(nam,"Array<Bool>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_bool(collection, v ? cass_true : cass_false)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Array<Float32>/*, let vs as Array<Float>*/:
            print(nam,"Array<Float32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_float(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Array<Float64>/*, let vs as Array<Double>*/:
            print(nam,"Array<Float64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_double(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Array<Int8> /*, let vs as Array<Int>*/:
            print(nam,"Array<Int8>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_int8(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Array<Int16> /*, let vs as Array<Int>*/:
            print(nam,"Array<Int16>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_int16(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Array<Int32> /*, let vs as Array<Int>*/:
            print(nam,"Array<Int32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_int32(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Array<Int64> /*, let vs as Array<Int>*/:
            print(nam,"Array<Int64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_LIST, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for v in vs {
                cass_collection_append_int64(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)

        case let vs as Dictionary<String, String>:
            print(nam,"Dictionary<String, String>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_string(collection, k)
                cass_collection_append_string(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<String, Bool>:
            print(nam,"Dictionary<String, Bool>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_string(collection, k)
                cass_collection_append_bool(collection, v ? cass_true : cass_false)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<String, Float32>:
            print(nam,"Dictionary<String, Float32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_string(collection, k)
                cass_collection_append_float(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<String, Float64>:
            print(nam,"Dictionary<String, Float64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_string(collection, k)
                cass_collection_append_double(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<String, Int8>:
            print(nam,"Dictionary<String, Int8>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_string(collection, k)
                cass_collection_append_int8(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<String, Int16>:
            print(nam,"Dictionary<String, Int16>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_string(collection, k)
                cass_collection_append_int16(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<String, Int32>:
            print(nam,"Dictionary<String, Int32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_string(collection, k)
                cass_collection_append_int32(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<String, Int64>:
            print(nam,"Dictionary<String, Int64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_string(collection, k)
                cass_collection_append_int64(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Bool, String>:
            print(nam,"Dictionary<Bool, String>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_bool(collection, k ? cass_true : cass_false)
                cass_collection_append_string(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Bool, Bool>:
            print(nam,"Dictionary<Bool, Bool>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_bool(collection, k ? cass_true : cass_false)
                cass_collection_append_bool(collection, v ? cass_true : cass_false)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Bool, Float32>:
            print(nam,"Dictionary<Bool, Float32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_bool(collection, k ? cass_true : cass_false)
                cass_collection_append_float(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Bool, Float64>:
            print(nam,"Dictionary<Bool, Float64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_bool(collection, k ? cass_true : cass_false)
                cass_collection_append_double(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Bool, Int8>:
            print(nam,"Dictionary<Bool, Int8>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_bool(collection, k ? cass_true : cass_false)
                cass_collection_append_int8(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Bool, Int16>:
            print(nam,"Dictionary<Bool, Int16>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_bool(collection, k ? cass_true : cass_false)
                cass_collection_append_int16(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Bool, Int32>:
            print(nam,"Dictionary<Bool, Int32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_bool(collection, k ? cass_true : cass_false)
                cass_collection_append_int32(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Bool, Int64>:
            print(nam,"Dictionary<Bool, Int64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_bool(collection, k ? cass_true : cass_false)
                cass_collection_append_int64(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Float32, String>:
            print(nam,"Dictionary<Float32, String>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_float(collection, k)
                cass_collection_append_string(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Float32, Bool>:
            print(nam,"Dictionary<Float32, Bool>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_float(collection, k)
                cass_collection_append_bool(collection, v ? cass_true : cass_false)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Float32, Float32>:
            print(nam,"Dictionary<Float32, Float32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_float(collection, k)
                cass_collection_append_float(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Float32, Float64>:
            print(nam,"Dictionary<Float32, Float64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_float(collection, k)
                cass_collection_append_double(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Float32, Int8>:
            print(nam,"Dictionary<Float32, Int8>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_float(collection, k)
                cass_collection_append_int8(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Float32, Int16>:
            print(nam,"Dictionary<Float32, Int16>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_float(collection, k)
                cass_collection_append_int16(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Float32, Int32>:
            print(nam,"Dictionary<Float32, Int32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_float(collection, k)
                cass_collection_append_int32(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Float32, Int64>:
            print(nam,"Dictionary<Float32, Int64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_float(collection, k)
                cass_collection_append_int64(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Float64, String>:
            print(nam,"Dictionary<Float64, String>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_double(collection, k)
                cass_collection_append_string(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Float64, Bool>:
            print(nam,"Dictionary<Float64, Bool>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_double(collection, k)
                cass_collection_append_bool(collection, v ? cass_true : cass_false)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Float64, Float32>:
            print(nam,"Dictionary<Float64, Float32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_double(collection, k)
                cass_collection_append_float(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Float64, Float64>:
            print(nam,"Dictionary<Float64, Float64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_double(collection, k)
                cass_collection_append_double(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Float64, Int8>:
            print(nam,"Dictionary<Float64, Int8>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_double(collection, k)
                cass_collection_append_int8(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Float64, Int16>:
            print(nam,"Dictionary<Float64, Int16>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_double(collection, k)
                cass_collection_append_int16(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Float64, Int32>:
            print(nam,"Dictionary<Float64, Int32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_double(collection, k)
                cass_collection_append_int32(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Float64, Int64>:
            print(nam,"Dictionary<Float64, Int64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_double(collection, k)
                cass_collection_append_int64(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int8, String>:
            print(nam,"Dictionary<Int8, String>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int8(collection, k)
                cass_collection_append_string(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int8, Bool>:
            print(nam,"Dictionary<Int8, Bool>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int8(collection, k)
                cass_collection_append_bool(collection, v ? cass_true : cass_false)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int8, Float32>:
            print(nam,"Dictionary<Int8, Float32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int8(collection, k)
                cass_collection_append_float(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int8, Float64>:
            print(nam,"Dictionary<Int8, Float64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int8(collection, k)
                cass_collection_append_double(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int8, Int8>:
            print(nam,"Dictionary<Int8, Int8>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int8(collection, k)
                cass_collection_append_int8(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int8, Int16>:
            print(nam,"Dictionary<Int8, Int16>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int8(collection, k)
                cass_collection_append_int16(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int8, Int32>:
            print(nam,"Dictionary<Int8, Int32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int8(collection, k)
                cass_collection_append_int32(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int8, Int64>:
            print(nam,"Dictionary<Int8, Int64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int8(collection, k)
                cass_collection_append_int64(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int16, String>:
            print(nam,"Dictionary<Int16, String>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int16(collection, k)
                cass_collection_append_string(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int16, Bool>:
            print(nam,"Dictionary<Int16, Bool>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int16(collection, k)
                cass_collection_append_bool(collection, v ? cass_true : cass_false)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int16, Float32>:
            print(nam,"Dictionary<Int16, Float32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int16(collection, k)
                cass_collection_append_float(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int16, Float64>:
            print(nam,"Dictionary<Int16, Float64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int16(collection, k)
                cass_collection_append_double(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int16, Int8>:
            print(nam,"Dictionary<Int16, Int8>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int16(collection, k)
                cass_collection_append_int8(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int16, Int16>:
            print(nam,"Dictionary<Int16, Int16>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int16(collection, k)
                cass_collection_append_int16(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int16, Int32>:
            print(nam,"Dictionary<Int16, Int32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int16(collection, k)
                cass_collection_append_int32(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int16, Int64>:
            print(nam,"Dictionary<Int16, Int64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int16(collection, k)
                cass_collection_append_int64(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int32, String>:
            print(nam,"Dictionary<Int32, String>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int32(collection, k)
                cass_collection_append_string(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int32, Bool>:
            print(nam,"Dictionary<Int32, Bool>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int32(collection, k)
                cass_collection_append_bool(collection, v ? cass_true : cass_false)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int32, Float32>:
            print(nam,"Dictionary<Int32, Float32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int32(collection, k)
                cass_collection_append_float(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int32, Float64>:
            print(nam,"Dictionary<Int32, Float64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int32(collection, k)
                cass_collection_append_double(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int32, Int8>:
            print(nam,"Dictionary<Int32, Int8>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int32(collection, k)
                cass_collection_append_int8(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int32, Int16>:
            print(nam,"Dictionary<Int32, Int16>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int32(collection, k)
                cass_collection_append_int16(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int32, Int32>:
            print(nam,"Dictionary<Int32, Int32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int32(collection, k)
                cass_collection_append_int32(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int32, Int64>:
            print(nam,"Dictionary<Int32, Int64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int32(collection, k)
                cass_collection_append_int64(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int64, String>:
            print(nam,"Dictionary<Int64, String>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int64(collection, k)
                cass_collection_append_string(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int64, Bool>:
            print(nam,"Dictionary<Int64, Bool>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int64(collection, k)
                cass_collection_append_bool(collection, v ? cass_true : cass_false)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int64, Float32>:
            print(nam,"Dictionary<Int64, Float32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int64(collection, k)
                cass_collection_append_float(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int64, Float64>:
            print(nam,"Dictionary<Int64, Float64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int64(collection, k)
                cass_collection_append_double(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int64, Int8>:
            print(nam,"Dictionary<Int64, Int8>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int64(collection, k)
                cass_collection_append_int8(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int64, Int16>:
            print(nam,"Dictionary<Int64, Int16>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int64(collection, k)
                cass_collection_append_int16(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int64, Int32>:
            print(nam,"Dictionary<Int64, Int32>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int64(collection, k)
                cass_collection_append_int32(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)
        case let vs as Dictionary<Int64, Int64>:
            print(nam,"Dictionary<Int64, Int64>",vs)
            let collection = cass_collection_new(CASS_COLLECTION_TYPE_MAP, vs.count)
            defer {
                cass_collection_free(collection)
            }
            for (k, v) in vs {
                cass_collection_append_int64(collection, k)
                cass_collection_append_int64(collection, v)
            }
            cass_statement_bind_collection_by_name(statement, nam, collection)

        default:
            print("*** name=\(nam), type of=\(type(of:value!)), Any=\(value!)")
            fatalError("Invalid argument: name=\(nam), type of=\(type(of:value!)), Any=\(value!)")
        }
    }
}
/*
 XX(CASS_VALUE_TYPE_CUSTOM,  0x0000, "", "") \
 XX(CASS_VALUE_TYPE_ASCII,  0x0001, "ascii", "org.apache.cassandra.db.marshal.AsciiType") \
 XX(CASS_VALUE_TYPE_BIGINT,  0x0002, "bigint", "org.apache.cassandra.db.marshal.LongType") \
 XX(CASS_VALUE_TYPE_BLOB,  0x0003, "blob", "org.apache.cassandra.db.marshal.BytesType") \
 XX(CASS_VALUE_TYPE_BOOLEAN,  0x0004, "boolean", "org.apache.cassandra.db.marshal.BooleanType") \
 XX(CASS_VALUE_TYPE_COUNTER,  0x0005, "counter", "org.apache.cassandra.db.marshal.CounterColumnType") \
 XX(CASS_VALUE_TYPE_DECIMAL,  0x0006, "decimal", "org.apache.cassandra.db.marshal.DecimalType") \
 XX(CASS_VALUE_TYPE_DOUBLE,  0x0007, "double", "org.apache.cassandra.db.marshal.DoubleType") \
 XX(CASS_VALUE_TYPE_FLOAT,  0x0008, "float", "org.apache.cassandra.db.marshal.FloatType") \
 XX(CASS_VALUE_TYPE_INT,  0x0009, "int", "org.apache.cassandra.db.marshal.Int32Type") \
 XX(CASS_VALUE_TYPE_TEXT,  0x000A, "text", "org.apache.cassandra.db.marshal.UTF8Type") \
 XX(CASS_VALUE_TYPE_TIMESTAMP,  0x000B, "timestamp", "org.apache.cassandra.db.marshal.TimestampType") \
 XX(CASS_VALUE_TYPE_UUID,  0x000C, "uuid", "org.apache.cassandra.db.marshal.UUIDType") \
 XX(CASS_VALUE_TYPE_VARCHAR,  0x000D, "varchar", "") \
 XX(CASS_VALUE_TYPE_VARINT,  0x000E, "varint", "org.apache.cassandra.db.marshal.IntegerType") \
 XX(CASS_VALUE_TYPE_TIMEUUID,  0x000F, "timeuuid", "org.apache.cassandra.db.marshal.TimeUUIDType") \
 XX(CASS_VALUE_TYPE_INET,  0x0010, "inet", "org.apache.cassandra.db.marshal.InetAddressType") \
 XX(CASS_VALUE_TYPE_DATE,  0x0011, "date", "org.apache.cassandra.db.marshal.SimpleDateType") \
 XX(CASS_VALUE_TYPE_TIME,  0x0012, "time", "org.apache.cassandra.db.marshal.TimeType") \
 XX(CASS_VALUE_TYPE_SMALL_INT,  0x0013, "smallint", "org.apache.cassandra.db.marshal.ShortType") \
 XX(CASS_VALUE_TYPE_TINY_INT,  0x0014, "tinyint", "org.apache.cassandra.db.marshal.ByteType") \
 XX(CASS_VALUE_TYPE_DURATION,  0x0015, "duration", "org.apache.cassandra.db.marshal.DurationType") \
 XX(CASS_VALUE_TYPE_LIST,  0x0020, "list", "org.apache.cassandra.db.marshal.ListType") \
 XX(CASS_VALUE_TYPE_MAP,  0x0021, "map", "org.apache.cassandra.db.marshal.MapType") \
 XX(CASS_VALUE_TYPE_SET,  0x0022, "set", "org.apache.cassandra.db.marshal.SetType") \
 XX(CASS_VALUE_TYPE_UDT,  0x0030, "", "") \
 XX(CASS_VALUE_TYPE_TUPLE,  0x0031, "tuple", "org.apache.cassandra.db.marshal.TupleType")
 */
func get_value(_ val_: OpaquePointer?) -> Any? {
    if let val = val_ {
        let typ = cass_value_type(val)
        //print("=== typ=\(typ) val=\(val)")
        switch typ {
        case CASS_VALUE_TYPE_VARCHAR:
            var data: UnsafePointer<Int8>?
            var len: Int = 0
            cass_value_get_string(val, &data, &len)
            let res = utf8_string(data: data, len: len)
            return res!
        case CASS_VALUE_TYPE_BOOLEAN:
            var res = cass_false
            cass_value_get_bool(val , &res)
            return cass_true == res
        case CASS_VALUE_TYPE_FLOAT:
            var res: Float32 = 0
            cass_value_get_float(val, &res)
            return res
        case CASS_VALUE_TYPE_DOUBLE:
            var res: Float64 = 0
            cass_value_get_double(val, &res)
            return res
        case CASS_VALUE_TYPE_INT:
            var res: Int32 = 0
            cass_value_get_int32(val, &res)
            return res
        case CASS_VALUE_TYPE_BIGINT:
            var res: Int64 = 0
            cass_value_get_int64(val, &res)
            return res
        case CASS_VALUE_TYPE_TIMEUUID, CASS_VALUE_TYPE_UUID:
            var cass_uuid = CassUuid()
            cass_value_get_uuid(val, &cass_uuid)
            let res = uuid(cass_uuid: &cass_uuid)
            return res
        case CASS_VALUE_TYPE_BLOB:
            var data: UnsafePointer<UInt8>?
            var len: Int = 0
            cass_value_get_bytes(val, &data, &len)
            let res = Array(UnsafeBufferPointer(start: data, count: len))
            return res
        case CASS_VALUE_TYPE_TIMESTAMP:
            var timestamp: Int64 = 0
            cass_value_get_int64(val, &timestamp)
            let res = Date(timeIntervalSince1970: TimeInterval(timestamp) / 1000)
            return res
            /*case CASS_VALUE_TYPE_DECIMAL:
             var data: UnsafePointer<UInt8>?
             var len: Int = 0
             var scale: Int32 = 0
             cass_value_get_decimal(val, &data, &len, &scale)
             let buf = Array(UnsafeBufferPointer(start: data, count: len).reversed())
             let bytesPointer = UnsafeMutableRawPointer.allocate(bytes: 8, alignedTo: 8)
             defer {
             bytesPointer.deallocate(bytes: 8, alignedTo: 8)
             }
             bytesPointer.initializeMemory(as: UInt64.self, to: 0)
             bytesPointer.copyBytes(from: buf, count: len)
             print("<<< buf=\(buf)")
             let f = Int64(1 << (8*len))
             let pu = bytesPointer.bindMemory(to: Int64.self, capacity: 1)
             let u = pu.pointee  > f >> 1 ? pu.pointee - f : pu.pointee
             let res = 0 > u
             ? Decimal(sign:.minus, exponent: -Int(scale), significand: Decimal(-u))
             : Decimal(sign:.plus, exponent: -Int(scale), significand: Decimal(u))
             return res*/

        case CASS_VALUE_TYPE_SET:
            let sub_type = cass_value_primary_sub_type(val)
            var res: Set<AnyHashable>
            switch sub_type {
            case CASS_VALUE_TYPE_VARCHAR:
                res = Set<String>()
            case CASS_VALUE_TYPE_BOOLEAN:
                res = Set<Bool>()
            case CASS_VALUE_TYPE_FLOAT:
                res = Set<Float32>()
            case CASS_VALUE_TYPE_DOUBLE:
                res = Set<Float64>()
            case CASS_VALUE_TYPE_INT:
                res = Set<Int32>()
            case CASS_VALUE_TYPE_BIGINT:
                res = Set<Int64>()
            case CASS_VALUE_TYPE_TIMEUUID, CASS_VALUE_TYPE_UUID:
                res = Set<UUID>()
            case CASS_VALUE_TYPE_TIMESTAMP:
                res = Set<Date>()
            default:
                //return nil
                fatalError("Invalid argument: type=\(typ) value=\(val)")
            }
            let it = CollectionIterator(val)
            for v in it {
                res.insert(v as! AnyHashable)
            }
            return res
        case CASS_VALUE_TYPE_LIST:
            let sub_type = cass_value_primary_sub_type(val)
            var res: Array<AnyHashable>
            switch sub_type {
            case CASS_VALUE_TYPE_VARCHAR:
                res = Array<String>()
            case CASS_VALUE_TYPE_BOOLEAN:
                res = Array<Bool>()
            case CASS_VALUE_TYPE_FLOAT:
                res = Array<Float32>()
            case CASS_VALUE_TYPE_DOUBLE:
                res = Array<Float64>()
            case CASS_VALUE_TYPE_INT:
                res = Array<Int32>()
            case CASS_VALUE_TYPE_BIGINT:
                res = Array<Int64>()
            case CASS_VALUE_TYPE_TIMEUUID, CASS_VALUE_TYPE_UUID:
                res = Array<UUID>()
            case CASS_VALUE_TYPE_TIMESTAMP:
                res = Array<Date>()
            default:
                //return nil
                fatalError("Invalid argument: type=\(typ) value=\(val)")
            }
            let it = CollectionIterator(val)
            for v in it {
                res.append(v as! AnyHashable)
            }
            return res
        case CASS_VALUE_TYPE_MAP:
            let key_type = cass_value_primary_sub_type(val)
            let val_type = cass_value_secondary_sub_type(val)
            var res: Dictionary<AnyHashable, Any?>
            switch key_type {
            case CASS_VALUE_TYPE_VARCHAR:
                switch val_type {
                case CASS_VALUE_TYPE_VARCHAR:
                    res = Dictionary<String, String?>()
                case CASS_VALUE_TYPE_BOOLEAN:
                    res = Dictionary<String, Bool?>()
                case CASS_VALUE_TYPE_FLOAT:
                    res = Dictionary<String, Float32?>()
                case CASS_VALUE_TYPE_DOUBLE:
                    res = Dictionary<String, Float64?>()
                case CASS_VALUE_TYPE_INT:
                    res = Dictionary<String, Int32?>()
                case CASS_VALUE_TYPE_BIGINT:
                    res = Dictionary<String, Int64?>()
                case CASS_VALUE_TYPE_TIMEUUID, CASS_VALUE_TYPE_UUID:
                    res = Dictionary<String, UUID?>()
                case CASS_VALUE_TYPE_TIMESTAMP:
                    res = Dictionary<String, Date?>()
                default:
                    //return nil
                    fatalError("Invalid argument: type=\(typ) value=\(val)")
                }
            case CASS_VALUE_TYPE_BOOLEAN:
                switch val_type {
                case CASS_VALUE_TYPE_VARCHAR:
                    res = Dictionary<Bool, String?>()
                case CASS_VALUE_TYPE_BOOLEAN:
                    res = Dictionary<Bool, Bool?>()
                case CASS_VALUE_TYPE_FLOAT:
                    res = Dictionary<Bool, Float32?>()
                case CASS_VALUE_TYPE_DOUBLE:
                    res = Dictionary<Bool, Float64?>()
                case CASS_VALUE_TYPE_INT:
                    res = Dictionary<Bool, Int32?>()
                case CASS_VALUE_TYPE_BIGINT:
                    res = Dictionary<Bool, Int64?>()
                case CASS_VALUE_TYPE_TIMEUUID, CASS_VALUE_TYPE_UUID:
                    res = Dictionary<Bool, UUID?>()
                case CASS_VALUE_TYPE_TIMESTAMP:
                    res = Dictionary<Bool, Date?>()
                default:
                    //return nil
                    fatalError("Invalid argument: type=\(typ) value=\(val)")
                }
            case CASS_VALUE_TYPE_FLOAT:
                switch val_type {
                case CASS_VALUE_TYPE_VARCHAR:
                    res = Dictionary<Float, String?>()
                case CASS_VALUE_TYPE_BOOLEAN:
                    res = Dictionary<Float, Bool?>()
                case CASS_VALUE_TYPE_FLOAT:
                    res = Dictionary<Float, Float32?>()
                case CASS_VALUE_TYPE_DOUBLE:
                    res = Dictionary<Float, Float64?>()
                case CASS_VALUE_TYPE_INT:
                    res = Dictionary<Float, Int32?>()
                case CASS_VALUE_TYPE_BIGINT:
                    res = Dictionary<Float, Int64?>()
                case CASS_VALUE_TYPE_TIMEUUID, CASS_VALUE_TYPE_UUID:
                    res = Dictionary<Float, UUID?>()
                case CASS_VALUE_TYPE_TIMESTAMP:
                    res = Dictionary<Float, Date?>()
                default:
                    //return nil
                    fatalError("Invalid argument: type=\(typ) value=\(val)")
                }
            case CASS_VALUE_TYPE_DOUBLE:
                switch val_type {
                case CASS_VALUE_TYPE_VARCHAR:
                    res = Dictionary<Double, String?>()
                case CASS_VALUE_TYPE_BOOLEAN:
                    res = Dictionary<Double, Bool?>()
                case CASS_VALUE_TYPE_FLOAT:
                    res = Dictionary<Double, Float32?>()
                case CASS_VALUE_TYPE_DOUBLE:
                    res = Dictionary<Double, Float64?>()
                case CASS_VALUE_TYPE_INT:
                    res = Dictionary<Double, Int32?>()
                case CASS_VALUE_TYPE_BIGINT:
                    res = Dictionary<Double, Int64?>()
                case CASS_VALUE_TYPE_TIMEUUID, CASS_VALUE_TYPE_UUID:
                    res = Dictionary<Double, UUID?>()
                case CASS_VALUE_TYPE_TIMESTAMP:
                    res = Dictionary<Double, Date?>()
                default:
                    //return nil
                    fatalError("Invalid argument: type=\(typ) value=\(val)")
                }
            case CASS_VALUE_TYPE_INT:
                switch val_type {
                case CASS_VALUE_TYPE_VARCHAR:
                    res = Dictionary<Int32, String?>()
                case CASS_VALUE_TYPE_BOOLEAN:
                    res = Dictionary<Int32, Bool?>()
                case CASS_VALUE_TYPE_FLOAT:
                    res = Dictionary<Int32, Float32?>()
                case CASS_VALUE_TYPE_DOUBLE:
                    res = Dictionary<Int32, Float64?>()
                case CASS_VALUE_TYPE_INT:
                    res = Dictionary<Int32, Int32?>()
                case CASS_VALUE_TYPE_BIGINT:
                    res = Dictionary<Int32, Int64?>()
                case CASS_VALUE_TYPE_TIMEUUID, CASS_VALUE_TYPE_UUID:
                    res = Dictionary<Int32, UUID?>()
                case CASS_VALUE_TYPE_TIMESTAMP:
                    res = Dictionary<Int32, Date?>()
                default:
                    //return nil
                    fatalError("Invalid argument: type=\(typ) value=\(val)")
                }
            case CASS_VALUE_TYPE_BIGINT:
                switch val_type {
                case CASS_VALUE_TYPE_VARCHAR:
                    res = Dictionary<Int64, String?>()
                case CASS_VALUE_TYPE_BOOLEAN:
                    res = Dictionary<Int64, Bool?>()
                case CASS_VALUE_TYPE_FLOAT:
                    res = Dictionary<Int64, Float32?>()
                case CASS_VALUE_TYPE_DOUBLE:
                    res = Dictionary<Int64, Float64?>()
                case CASS_VALUE_TYPE_INT:
                    res = Dictionary<Int64, Int32?>()
                case CASS_VALUE_TYPE_BIGINT:
                    res = Dictionary<Int64, Int64?>()
                case CASS_VALUE_TYPE_TIMEUUID, CASS_VALUE_TYPE_UUID:
                    res = Dictionary<Int64, UUID?>()
                case CASS_VALUE_TYPE_TIMESTAMP:
                    res = Dictionary<Int64, Date?>()
                default:
                    //return nil
                    fatalError("Invalid argument: type=\(typ) value=\(val)")
                }
            case CASS_VALUE_TYPE_TIMEUUID, CASS_VALUE_TYPE_UUID:
                switch val_type {
                case CASS_VALUE_TYPE_VARCHAR:
                    res = Dictionary<UUID, String?>()
                case CASS_VALUE_TYPE_BOOLEAN:
                    res = Dictionary<UUID, Bool?>()
                case CASS_VALUE_TYPE_FLOAT:
                    res = Dictionary<UUID, Float32?>()
                case CASS_VALUE_TYPE_DOUBLE:
                    res = Dictionary<UUID, Float64?>()
                case CASS_VALUE_TYPE_INT:
                    res = Dictionary<UUID, Int32?>()
                case CASS_VALUE_TYPE_BIGINT:
                    res = Dictionary<UUID, Int64?>()
                case CASS_VALUE_TYPE_TIMEUUID, CASS_VALUE_TYPE_UUID:
                    res = Dictionary<UUID, UUID?>()
                case CASS_VALUE_TYPE_TIMESTAMP:
                    res = Dictionary<UUID, Date?>()
                default:
                    //return nil
                    fatalError("Invalid argument: type=\(typ) value=\(val)")
                }
            case CASS_VALUE_TYPE_TIMESTAMP:
                switch val_type {
                case CASS_VALUE_TYPE_VARCHAR:
                    res = Dictionary<Date, String?>()
                case CASS_VALUE_TYPE_BOOLEAN:
                    res = Dictionary<Date, Bool?>()
                case CASS_VALUE_TYPE_FLOAT:
                    res = Dictionary<Date, Float32?>()
                case CASS_VALUE_TYPE_DOUBLE:
                    res = Dictionary<Date, Float64?>()
                case CASS_VALUE_TYPE_INT:
                    res = Dictionary<Date, Int32?>()
                case CASS_VALUE_TYPE_BIGINT:
                    res = Dictionary<Date, Int64?>()
                case CASS_VALUE_TYPE_TIMEUUID, CASS_VALUE_TYPE_UUID:
                    res = Dictionary<Date, UUID?>()
                case CASS_VALUE_TYPE_TIMESTAMP:
                    res = Dictionary<Date, Date?>()
                default:
                    //return nil
                    fatalError("Invalid argument: type=\(typ) value=\(val)")
                }
            default:
                //return nil
                fatalError("Invalid argument: type=\(typ) value=\(val)")
            }
            let it = MapIterator(val)
            for (k, v) in it {
                res[k] = v
            }
            return res
        default:
            //return nil
            fatalError("Invalid argument: type=\(typ) value=\(val)")
        }
    } else {
        return nil
    }
}

func futureMessage(_ future: OpaquePointer) -> String? {
    let rc = cass_future_error_code(future)
    if (CASS_OK != rc) {
        defer {
            cass_future_free(future)
        }
        if let msg = error_string(future) {
            return msg
        } else {
            return "Execution error \(rc)"
        }
    }
    return nil
}

