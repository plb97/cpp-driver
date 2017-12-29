//
//  resultset.swift
//  cpp-driver
//
//  Created by Philippe on 16/12/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

import Foundation

public
class ResultSet: Error {
    let err_: String?
    let result_: OpaquePointer?
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
class Row {
    let row: OpaquePointer!
    init(_ row: OpaquePointer!) {
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
    let iterator_: OpaquePointer?
    init(_ result_: OpaquePointer?) {
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
    let iterator: OpaquePointer!
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
    let iterator: OpaquePointer!
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

