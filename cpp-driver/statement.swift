//
//  statement.swift
//  cpp-driver
//
//  Created by Philippe on 16/12/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

import Foundation

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
    var prepared_: OpaquePointer?
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
    func stmt(_ lst: [Any?]) -> OpaquePointer! {
        if let prepared = prepared_ {
            if let statement = cass_prepared_bind(prepared) {
                bind(statement, lst: lst)
                return statement
            }
        }
        return nil
    }
    func stmt(map: [String: Any?]) -> OpaquePointer! {
        if let prepared = prepared_ {
            if let statement = cass_prepared_bind(prepared) {
                bind(statement, map: map)
                return statement
            }
        }
        return nil
    }
}

