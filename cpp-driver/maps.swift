//
//  maps.swift
//  cpp-driver
//
//  Created by Philippe on 30/10/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

fileprivate
let KEY = "test"

fileprivate
func getSession() -> Session {
    let session = Session()
    _ = Cluster().setContactPoints("127.0.0.1").setCredentials().connect(session).check()
    return session
}

fileprivate
func create_keyspace(session: Session) -> () {
    print("create_keyspace...")
    let query = """
    CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {
                           'class': 'SimpleStrategy', 'replication_factor': '3' };
    """
    let future = session.execute(SimpleStatement(query))
    print("...create_keyspace")
    _ = future.check()
}
fileprivate
func create_table(session: Session) -> () {
    print("create_table...")
    let query = """
    CREATE TABLE IF NOT EXISTS examples.maps (key text,
                items map<text, int>,
                PRIMARY KEY (key));
    """
    let future = session.execute(SimpleStatement(query))
    print("...create_table")
    _ = future.check()
}
fileprivate
func insert_into(_ session: Session,_ key: String,_ items: Dictionary<String, Int32>) -> () {
    print("insert_into_maps...")
    let query = "INSERT INTO examples.maps (key, items) VALUES (?, ?);"
    let statement = SimpleStatement(query,
                                    key,
                                    items)
    let future = session.execute(statement)
    print("...insert_into_maps")
    _ = future.check()
}
fileprivate
func select_from(_ session: Session,_ key: String) -> ResultSet {
    print("select_from_maps...")
    let query = "SELECT key, items FROM examples.maps WHERE key = ?;"
    let statement = SimpleStatement(query,key)
    let rs = ResultSet(session.execute(statement))
    print("...select_from_maps")
    _ = rs.check()
    return rs
}

func maps() {
    print("maps...")
    let session = getSession()
    create_keyspace(session: session)
    create_table(session: session)

    let items: Dictionary<String, Int32> = [
        "apple":1,
        "orange":2,
        "banana":3,
        "mango":4
    ]

    insert_into(session,KEY, items)

    let rs = select_from(session, KEY)
    print("count=\(rs.count())")
    print("column_count=\(rs.column_count())")
    /*print("first")
     if let row = rs.first() {
         let key = row.any(0) as! String
         print("key=\(key)")
         let map = row.any(1) as! Dictionary<String, Int32>
         for (k, v) in map {
             print("\(k) -> \(v)")
         }
     }*/
    print("rows")
    for row in rs.rows() {
        let key = row.any(0) as! String
        print("key=\(key)")
        let map = row.any(1) as! Dictionary<String, Int32>
        for (k, v) in map {
            print("\(k) -> \(v)")
        }
        /*for k in map.keys.sorted() {
         let v = map[k]!
         print("\(k) -> \(v)")
         }*/
    }

    print("...maps")
}
