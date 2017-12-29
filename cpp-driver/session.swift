//
//  session.swift
//  cpp-driver
//
//  Created by Philippe on 16/12/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

import Foundation

public
class Session {
    let session: OpaquePointer
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
class Batch {
    let batch: OpaquePointer!
    init(_ type: CassBatchType) {
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

