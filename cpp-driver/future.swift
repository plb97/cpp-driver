//
//  future.swift
//  cpp-driver
//
//  Created by Philippe on 16/12/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

import Foundation

public
class FutureBase: Error {
    var future_: OpaquePointer?
    var err_: String?
    init() {
        future_ = nil
        err_ = nil
        print("init FutureBase listener")
    }
    init(_ future: OpaquePointer) {
        err_ = futureMessage(future)
        if nil == err_ {
            future_ = future
        } else {
            future_ = nil
        }
        print("init FutureBase")
    }
    init(_ future: OpaquePointer, listener: Listener) {
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
    override init(_ future: OpaquePointer) {
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

