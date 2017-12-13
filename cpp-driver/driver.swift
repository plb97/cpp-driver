//
//  driver.swift
//  cpp-driver
//
//  Created by Philippe on 21/10/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

import Foundation

// http://docs.datastax.com/en/developer/cpp-driver/2.7/topics/security/
public
class Cluster {
    fileprivate let cluster: OpaquePointer = cass_cluster_new()
    init(_ hosts: String) {
        print("init Cluster")
        cass_cluster_set_contact_points(cluster, hosts)
    }
    /*init(_ hosts: String, authenticator: Authenticator) {
        print("init Cluster")
        cass_cluster_set_contact_points(cluster, hosts)
        cass_cluster_set_authenticator_callbacks(cluster, &authenticator.callbacks, nil, &authenticator.data)
    }*/
    deinit {
        print("deinit Cluster")
        cass_cluster_free(cluster)
    }
    public func connect(_ session: Session) -> Future {
        return session.connect(self)
    }
    public func connect(_ session: Session, listener: Listener) -> FutureBase {
        return session.connect(self, listener: listener)
    }
}
public
class BasicCluster: Cluster {
    init(_ hosts: String, username: String = "cassandra", password: String = "cassandra") {
        print("init BasicCluster")
        super.init(hosts)
        cass_cluster_set_credentials(cluster, username, password)
    }
    deinit {
        print("init BasicCluster")
    }
}

typealias callback_t = @convention(c) (OpaquePointer?, UnsafeMutableRawPointer?) -> ()
public typealias Listener_f = (FutureBase?, UnsafeMutableRawPointer?) -> ()
public
struct Listener {
    public let callback: Listener_f
    public let data_: UnsafeMutableRawPointer?
    init(_ callback: @escaping Listener_f,_ data_: UnsafeMutableRawPointer? = nil) {
        self.callback = callback
        self.data_ = data_
    }
}
//@_silgen_name("callback_f")
func callback_f(future_: OpaquePointer?, data_: UnsafeMutableRawPointer?) -> () {
    print("callback_f...")
    if let future = future_ {
        if let data = data_ {
            DispatchQueue.global().async {
                let listener = data.bindMemory(to: Listener.self, capacity: 1).pointee
                defer {
                    data.deallocate(bytes: MemoryLayout<Listener>.stride, alignedTo: MemoryLayout<Listener>.alignment)
                }
                listener.callback(FutureBase(future), listener.data_)
            }
        }
    }
    print("...callback_f")
}

public typealias Authenticator_f = (Authenticator?, UnsafeMutableRawPointer?) -> ()
public typealias Authenticator_token_f = (Authenticator?, UnsafeMutableRawPointer?, String?) -> ()
private func ok(auth_: Authenticator? = nil,data_: UnsafeMutableRawPointer? = nil) -> () {}
private func ok_token(auth_: Authenticator? = nil,data_: UnsafeMutableRawPointer? = nil,token_: String? = nil) -> () {}
public
struct Authenticator {
    public let initial_callback: Authenticator_f
    public let challenge_callback: Authenticator_token_f
    public let success_callback: Authenticator_token_f
    public let cleanup_callback: Authenticator_f
    public let data_: UnsafeMutableRawPointer?
    init(initial_callback: @escaping Authenticator_f = ok,
         challenge_callback: @escaping Authenticator_token_f = ok_token,
         success_callback: @escaping Authenticator_token_f = ok_token,
         cleanup_callback: @escaping Authenticator_f = ok,
         _ data_: UnsafeMutableRawPointer? = nil) {
            self.initial_callback = initial_callback
            self.challenge_callback = challenge_callback
            self.success_callback = success_callback
            self.cleanup_callback = cleanup_callback
        self.data_ = data_
    }
}

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
class FutureBase: Error {
    var future_: OpaquePointer?
    var err_: String?
    fileprivate init() {
        future_ = nil
        err_ = nil
        print("init FutureBase listener")
    }
    fileprivate init(_ future: OpaquePointer) {
        err_ = futureMessage(future)
        if nil == err_ {
            future_ = future
        } else {
            future_ = nil
        }
        print("init FutureBase")
    }
    fileprivate init(_ future: OpaquePointer, listener: Listener) {
        let ptr = UnsafeMutablePointer<Listener>.allocate(capacity: MemoryLayout<Listener>.stride)
        ptr.initialize(to: listener)
        cass_future_set_callback(future, callback_f, ptr)
        print("init Future listener")
        //super.init()
    }
    deinit {
        print("deinit FutureBase")
    }
    public func error() -> String? {
        return err_
    }
    public func check(checker: Checker_f = checkError) -> Bool {
        return checker(self)
    }
}

public
class Future: FutureBase {
    fileprivate override init(_ future: OpaquePointer) {
        cass_future_wait(future)
        super.init(future)
        print("init Future")
    }
    deinit {
        print("deinit Future")
        if let future = future_ {
            defer {
                cass_future_free(future)
            }
        }
    }
}

public
class ResultSet: Error {
    fileprivate let err_: String?
    fileprivate let result_: OpaquePointer?
    init(_ future: FutureBase) {
        print("init ResultSet")
        err_ = future.err_
        if nil == err_ && nil != future.future_ {
            result_ = cass_future_get_result(future.future_!)
        } else {
            result_ = nil
        }
    }
    deinit {
        print("deinit ResultSet")
        if let result = result_ {
            cass_result_free(result)
        }
    }
    public func error() -> String? {
        return err_
    }
    public func check(checker: Checker_f = checkError) -> Bool {
        return checker(self)
    }
    public func count() -> Int {
        if let result = result_ {
            let ctr = cass_result_row_count(result)
            return ctr
        }
        return 0
    }
    public func column_count() -> Int {
        if let result = result_ {
            let ctr = cass_result_column_count(result)
            return ctr
        }
        return 0
    }
    public func first() -> Row? {
        if let result = result_ {
            if let row = cass_result_first_row(result) {
                return Row(row)
            }
        }
        return nil
    }
    public func rows() -> RowIterator {
        return RowIterator(result_)
    }
}


public
class Session {
    fileprivate let session: OpaquePointer
    init(_ session: OpaquePointer = cass_session_new()) {
        self.session = session
        print("init Session")
    }
    deinit {
        print("deinit Session")
        cass_session_close(session)
        cass_session_free(session)
    }
    public func connect(_ cluster: Cluster) -> Future {
        return Future(cass_session_connect(session, cluster.cluster))
    }
    public func execute(batch: Batch) -> Future {
        return Future(cass_session_execute_batch(session, batch.batch))
    }
    public func execute(_ statement: SimpleStatement) -> Future {
        return Future(cass_session_execute(session, statement.stmt()))
    }
    public func prepare(_ query: String) -> PreparedStatement {
        let stmt = PreparedStatement(cass_session_prepare(session, query))
        return stmt
    }
    public func connect(_ cluster: Cluster, listener: Listener) -> FutureBase {
        return FutureBase(cass_session_connect(session, cluster.cluster), listener: listener)
    }
    public func execute(_ statement: SimpleStatement, listener: Listener) -> FutureBase {
        return FutureBase(cass_session_execute(session, statement.stmt()), listener: listener)
    }
}

public
class CallbackParam {
    let future: OpaquePointer!
    let data: UnsafeMutableRawPointer?
    init(future: OpaquePointer!, data: UnsafeMutableRawPointer? = nil) {
        self.future = future
        self.data = data
    }
}
public
class Row {
    fileprivate let row: OpaquePointer!
    fileprivate init(_ row: OpaquePointer!) {
        print("init Row")
        self.row = row
    }
    deinit {
        print("deinit Row")
    }
    public func any(_ i: Int) -> Any? {
        let col = cass_row_get_column(row, i)
        let val = get_value(col)
        return val
    }
    public func any(name: String) -> Any? {
        let col = cass_row_get_column_by_name(row, name)
        let val = get_value(col)
        return val
    }
}
public
class RowIterator: Sequence, IteratorProtocol {
    fileprivate let iterator_: OpaquePointer?
    fileprivate init(_ result_: OpaquePointer?) {
        print("init RowIterator")
        if let result = result_ {
            iterator_ = cass_iterator_from_result(result)
        } else {
            iterator_ = nil
        }
    }
    deinit {
        print("deinit RowIterator")
        if let iterator = iterator_ {
            cass_iterator_free(iterator)
        }
    }
    public func next() -> Row? {
        if let iterator = iterator_ {
            guard cass_true == cass_iterator_next(iterator)
                else { return nil }
            if let row = cass_iterator_get_row(iterator) {
                return Row(row)
            } else {
                return nil
            }
        } else {
            return nil
        }
    }
}
public
class CollectionIterator: Sequence, IteratorProtocol {
    fileprivate let iterator: OpaquePointer!
    init(_ value: OpaquePointer!) {
        print("init CollectionIterator")
        iterator = cass_iterator_from_collection(value)
    }
    deinit {
        print("deinit CollectionIterator")
        cass_iterator_free(iterator)
    }
    public func next() -> Any? {
        guard cass_true == cass_iterator_next(iterator)
            else { return nil }
        if let val = cass_iterator_get_value(iterator) {
            let res = get_value(val)
            return res
        } else {
            return nil
        }
    }
}
public
class MapIterator: Sequence, IteratorProtocol {
    fileprivate let iterator: OpaquePointer!
    init(_ value: OpaquePointer!) {
        print("init MapIterator")
        iterator = cass_iterator_from_map(value)
    }
    deinit {
        print("deinit MapIterator")
        cass_iterator_free(iterator)
    }
    public func next() -> (key: AnyHashable, value: Any?)? {
        guard cass_true == cass_iterator_next(iterator)
            else { return nil }
        if let key = cass_iterator_get_map_key(iterator) {
            if let k_ = get_value(key)  {
                let k = k_ as! AnyHashable
                if let val = cass_iterator_get_map_value(iterator) {
                    if let v = get_value(val) {
                        return (key: k, value: v)
                    } else {
                        return (key: k, value: nil)
                    }
                } else {
                    return (key: k, value: nil)
                }
            } else {
                return nil
            }
        } else {
            return nil
        }
    }
}
public
class Generator {
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

public
class Batch {
    let batch: OpaquePointer!
    fileprivate init(_ type: CassBatchType) {
        print("init Batch")
        batch = cass_batch_new(type)
    }
    deinit {
        print("deinit Batch")
        cass_batch_free(batch)
    }
    public func add(_ statement: SimpleStatement) {
        if let stmt = statement.stmt() {
            defer {
                cass_statement_free(stmt)
            }
            cass_batch_add_statement(batch, stmt)
        }
    }
    public func add(prepared: PreparedStatement,_ lst: [Any?]) {
        if let statement = prepared.stmt(lst) {
            defer {
                cass_statement_free(statement)
            }
            cass_batch_add_statement(batch, statement)
        }
    }
    public func add(prepared: PreparedStatement, map: [String: Any?]) {
        if let statement = prepared.stmt(map: map) {
            defer {
                cass_statement_free(statement)
            }
            cass_batch_add_statement(batch, statement)
        }
    }
}

public
class BatchLogged: Batch {
    init() {
        super.init(CASS_BATCH_TYPE_LOGGED)
    }
}
public
class BatchUnLogged: Batch {
    init() {
        super.init(CASS_BATCH_TYPE_UNLOGGED)
    }
}
public
class BatchCounter: Batch {
    init() {
        super.init(CASS_BATCH_TYPE_COUNTER)
    }
}

public
class SimpleStatement {
    private let query: String?
    private let _lst: [Any?]?
    private let _map: [String: Any?]?
    func stmt() -> OpaquePointer! {
        if let lst = _lst {
            let ctr = lst.count
//            print("ctr = \(ctr)")
            if let statement = cass_statement_new(query, ctr) {
                bind(statement, lst: lst)
                return statement
            }
        } else if let map = _map {
            let ctr = map.count
//            print("ctr = \(ctr)")
            if let statement = cass_statement_new(query, ctr) {
                bind(statement, map: map)
                return statement
            }
        }
        return nil
    }
    init(_ query: String,_ values: Any?...) {
        print("init SimpleStament")
        self.query = query
        self._lst = values
        self._map = nil
    }
    init(_ query: String, map: [String: Any?]) {
        print("init SimpleStament")
        self.query = query
        self._lst = nil
        self._map = map
    }
    deinit {
        print("deinit SimpleStament")
    }
}

public
class PreparedStatement: Error  {
    var err_: String?
    fileprivate var prepared_: OpaquePointer?
    init(_ future: OpaquePointer) {
        defer {
            cass_future_free(future)
        }
        cass_future_wait(future)
        err_ = futureMessage(future)
        if nil == err_ {
            prepared_ = cass_future_get_prepared(future)
        }
        print("init PreparedStatement")
    }
    deinit {
        print("deinit PreparedStatement")
        if let prepared = prepared_ {
            cass_prepared_free(prepared)
            prepared_ = nil
        }
    }
    public func error() -> String? {
        return err_
    }
    public func check(checker: Checker_f = checkError) -> Bool {
        return checker(self)
    }
    fileprivate func stmt(_ lst: [Any?]) -> OpaquePointer! {
        if let prepared = prepared_ {
            if let statement = cass_prepared_bind(prepared) {
                bind(statement, lst: lst)
                return statement
            }
        }
        return nil
    }
    fileprivate func stmt(map: [String: Any?]) -> OpaquePointer! {
        if let prepared = prepared_ {
            if let statement = cass_prepared_bind(prepared) {
                bind(statement, map: map)
                return statement
            }
        }
        return nil
    }
}

