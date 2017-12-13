//
//  batch.swift
//  cpp-driver
//
//  Created by Philippe on 25/10/2017.
//  Copyright © 2017 PLB. All rights reserved.
//

fileprivate
func getSession() -> Session {
    let session = Session()
    BasicCluster("127.0.0.1").connect(session).check()
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
    future.check()
}
fileprivate
func create_table(session: Session) -> () {
    print("create_table...")
    let query = """
    CREATE TABLE IF NOT EXISTS examples.pairs (key text,
                                              value text,
                                              PRIMARY KEY (key));
    """
    let future = session.execute(SimpleStatement(query))
    print("...create_table")
    future.check()
}
fileprivate
func prepare_statement(session: Session) -> PreparedStatement {
    let query = "INSERT INTO examples.pairs (key, value) VALUES (?, ?);"
    let prepared = session.prepare(query)
    prepared.check()
    return prepared
}
fileprivate
func insert_into(session: Session,_ pairs: [[String]]) -> () {
    print("insert_into...")
    let batch = BatchLogged()
    let prepared = prepare_statement(session: session)
    for pair in pairs {
        batch.add(prepared: prepared, pair)
    }
    batch.add(SimpleStatement("INSERT INTO examples.pairs (key, value) VALUES ('c', '3');"))
    batch.add(SimpleStatement("INSERT INTO examples.pairs (key, value) VALUES (?, ?);","d","4"))
    let future = session.execute(batch: batch)
    print("insert_into...")
    future.check()
}

func batch() {
    print("batch...")
    
    let pairs = [["a", "1"], ["b", "2"]]

    let session = getSession()
    create_keyspace(session: session)
    create_table(session: session)
    insert_into(session: session, pairs)
    print("...batch")
}
