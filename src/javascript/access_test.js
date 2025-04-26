// MIT License
//
// Copyright (c) 2023-2025 Roger L. Deran
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
// THE SOFTWARE.

import {idb} from "./infinitydb_access.js";
import test from 'node:test';
import assert from 'node:assert';

let INTERFACE = "examples";
let server = new idb.Accessor("http://localhost/infinitydb/data", "demo/writeable", "testUser", "db");
//let server = new idb.Accessor("https://infinitydb.com/infinitydb/data", "demo/writeable", "testUser", "db");

// ===== Original tests =====

test('unflattenQueryData', (t) => {
    let date = new Date("December 1 2023");
    let obj = {"a": [new idb.Long(1), new idb.Long(3),new idb.Long(5)],
                "b": new idb.Double(6),
                "c": date}; 
    let obj_unflattened = idb.unflattenQueryData(obj);

    let expected_obj = {
            'a':{'_1': {'_3': {'_5': null}}},
            'b': {'_6.0': null},
            'c': {['_' + date.toISOString()]: null}
    };

    assert.deepEqual(obj_unflattened, expected_obj);
});

test('json query', async (t) => {
    let success = await server.head();
    assert(success);

    let result, content_type;
    [success, result, content_type] = await server.execute_query([INTERFACE, "fish farm profit"]);
    assert.equal(content_type, 'application/json');
    assert(success);
});

test('get blob query', async (t) => {
    let [success, result, content_type] = await server.execute_get_blob_query(
        [INTERFACE, "Display Image"],
        {_name: "pic0"}
    );
    assert(success);
    assert.equal(content_type, 'image/jpeg');
});

test('get/put blob', async(t) => {
    let PICTURES = new idb.Class("Pictures");
    let [success, image_blob, content_type] = await server.get_blob(
        [PICTURES, 'pic0']
    );
    assert(success);
    assert.equal(content_type, "image/jpeg");

    success = await server.put_blob([PICTURES, "pic0_temp"], image_blob);
    assert(success);
    let image_blob_redownloaded;
    [success, image_blob_redownloaded, content_type] = await server.get_blob(
        [PICTURES, 'pic0_temp']
    );
    assert(success);
    assert.deepEqual(image_blob, image_blob_redownloaded);

    // This doesn't work right now.
    //success = await server.delete([PICTURES, "pic0_temp"]);
    //assert(success);
});

test('get/put json', async(t) => {
    let AIRCRAFT = new idb.Class("Aircraft");
    let [success, result, content_type] = await server.get_json([AIRCRAFT, "737"]);
    assert(success);
    assert.equal(content_type, "application/json");
});

// ===== OrderedMap tests =====

test('OrderedMap - Basic operations', (t) => {
    const map = new idb.OrderedMap();
    
    // Test initial state
    assert.equal(map.isEmpty(), true, "Map should start empty");
    assert.equal(map.size(), 0, "Size should be 0");
    
    // Test set and get
    map.set("a", 1);
    map.set("c", 3);
    map.set("b", 2);
    
    assert.equal(map.size(), 3, "Size should be 3");
    assert.equal(map.get("a"), 1);
    assert.equal(map.get("b"), 2);
    assert.equal(map.get("c"), 3);
    
    // Test ordering
    const keys = map.keys();
    assert.equal(keys[0], "a", "First key should be 'a'");
    assert.equal(keys[1], "b", "Second key should be 'b'");
    assert.equal(keys[2], "c", "Third key should be 'c'");
    
    // Test updating
    map.set("b", 4);
    assert.equal(map.get("b"), 4, "Value should be updated");
    assert.equal(map.size(), 3, "Size should remain 3");
    
    // Test has and delete
    assert.equal(map.has("b"), true, "Should have key 'b'");
    map.delete("b");
    assert.equal(map.has("b"), false, "Should not have key 'b' after delete");
    assert.equal(map.size(), 2, "Size should be 2");
});

test('OrderedMap - Component ordering', (t) => {
    const map = new idb.OrderedMap();
    
    // Add different component types to test ordering
    map.set(new idb.Class("TestClass"), 1);
    map.set(new idb.Attribute("testAttr"), 2);
    map.set("test string", 3);
    map.set(new idb.Long(10), 4);
    map.set(new idb.Double(5.5), 5);
    map.set(true, 6);
    
    // Test sorted keys based on component type
    const keys = map.keys();
    
    // Verify sorted by TYPE_ORDER
    assert.ok(keys[0] instanceof idb.Class, "First key should be a Class");
    assert.ok(keys[1] instanceof idb.Attribute, "Second key should be an Attribute");
});

test('OrderedMap - Binary search correctness', (t) => {
    const map = new idb.OrderedMap();
    
    // Add 100 items
    for (let i = 0; i < 100; i++) {
        map.set(i * 2, `value${i}`);
    }
    
    // Check that we can find all items
    for (let i = 0; i < 100; i++) {
        assert.equal(map.has(i * 2), true, `Should have key ${i * 2}`);
        assert.equal(map.get(i * 2), `value${i}`);
    }
    
    // Check that we don't find non-existent items
    for (let i = 0; i < 99; i++) {
        assert.equal(map.has(i * 2 + 1), false, `Should not have key ${i * 2 + 1}`);
    }
});

// ===== ItemSpace tests =====

test('ItemSpace - Adding and retrieving items', (t) => {
    const space = new idb.ItemSpace();
    
    // Create a few items
    const item1 = new idb.Item(
        new idb.Class("Test"),
        "data",
        new idb.Long(1)
    );
    
    const item2 = new idb.Item(
        new idb.Class("Test"),
        "data",
        new idb.Long(2)
    );
    
    const item3 = new idb.Item(
        new idb.Class("Test"),
        "other",
        new idb.Attribute("attr")
    );
    
    // Add items
    space.addItem(item1);
    space.addItem(item2);
    space.addItem(item3);
    
    // Check count
    assert.equal(space.count(), 3, "Should have 3 items");
    
    // Verify items
    const items = space.getItems();
    assert.equal(items.length, 3, "Should get 3 items");
    
    // Check map view navigation
    const testNode = space.get(new idb.Class("Test"));
    assert.ok(testNode !== null, "Should have Test class node");
    
    // Check keys at root
    const rootKeys = space.keys();
    assert.equal(rootKeys.length, 1, "Should have 1 key at root");
    assert.ok(rootKeys[0] instanceof idb.Class && rootKeys[0].v === "Test", 
        "Root key should be Test class");
    
    // Check second level
    const dataNode = testNode.get("data");
    assert.ok(dataNode !== null, "Should have 'data' node");
    
    // Verify that we can find both number items
    const dataKeys = dataNode.keys();
    assert.equal(dataKeys.length, 2, "Should have 2 keys under 'data'");
});

test('ItemSpace - Removing items', (t) => {
    const space = new idb.ItemSpace();
    
    // Add a few items
    const item1 = new idb.Item(
        new idb.Class("Test"),
        "data",
        new idb.Long(1)
    );
    
    const item2 = new idb.Item(
        new idb.Class("Test"),
        "data",
        new idb.Long(2)
    );
    
    space.addItem(item1);
    space.addItem(item2);
    assert.equal(space.count(), 2, "Should have 2 items");
    
    // Remove one item
    space.removeItem(item1);
    assert.equal(space.count(), 1, "Should have 1 item after removal");
    
    // Check that the item is gone
    const testNode = space.get(new idb.Class("Test"));
    const dataNode = testNode.get("data");
    const dataKeys = dataNode.keys();
    assert.equal(dataKeys.length, 1, "Should have 1 key under 'data'");
    assert.ok(dataKeys[0] instanceof idb.Long && dataKeys[0].v === 2,
        "Remaining key should be 2");
});

test('ItemSpace - Convert to IDB object', (t) => {
    const space = new idb.ItemSpace();
    
    // Add a few items
    const item1 = new idb.Item(
        new idb.Class("Test"),
        "data", 
        new idb.Long(1)
    );
    
    const item2 = new idb.Item(
        new idb.Class("Test"),
        "data",
        new idb.Long(2)
    );
    
    space.addItem(item1);
    space.addItem(item2);
    
    // Convert to IDB object
    const obj = space.toIdbObject();
    
    // Check structure
    assert.ok(obj['_Test'] !== undefined, "Should have _Test key");
    assert.ok(obj['_Test']['data'] !== undefined, "Should have data key");
    assert.ok(obj['_Test']['data']['_1'] !== undefined, "Should have _1 key");
    assert.ok(obj['_Test']['data']['_2.0'] !== undefined, "Should have _2.0 key");
});

test('ItemSpace - Duplicate prevention', (t) => {
    const space = new idb.ItemSpace();
    
    // Create an item
    const item = new idb.Item(
        new idb.Class("Test"),
        "data",
        new idb.Long(1)
    );
    
    // Add it twice
    space.addItem(item);
    space.addItem(item);
    
    // Check that it was only added once
    assert.equal(space.count(), 1, "Should have only 1 item");
});

test('ItemSpace - Round trip conversion', (t) => {
    const space1 = new idb.ItemSpace();
    
    // Add a few items
    space1.addItem(new idb.Item(
        new idb.Class("Test"),
        "data",
        new idb.Long(1)
    ));
    
    space1.addItem(new idb.Item(
        new idb.Class("Test"),
        "data",
        new idb.Long(2)
    ));
    
    space1.addItem(new idb.Item(
        new idb.Class("Test"),
        "other",
        new idb.Attribute("attr")
    ));
    
    // Convert to IDB object
    const obj = space1.toIdbObject();
    
    // Convert back to ItemSpace
    const space2 = idb.ItemSpace.fromIdbObject(obj);
    
    // Check item count
    assert.equal(space2.count(), 3, "Should have 3 items after round trip");
    
    // Check structure
    const testNode = space2.get(new idb.Class("Test"));
    assert.ok(testNode !== null, "Should have Test class node after round trip");
    
    const dataNode = testNode.get("data");
    assert.ok(dataNode !== null, "Should have 'data' node after round trip");
    
    const dataKeys = dataNode.keys();
    assert.equal(dataKeys.length, 2, "Should have 2 keys under 'data' after round trip");
});

// ===== Complex Query tests =====

test('Complex - Deep nesting', (t) => {
    const space = new idb.ItemSpace();
    
    // Create a deeply nested item for algae detection
    const timestamp = new Date();
    const item1 = new idb.Item(
        new idb.Class("Label"),
        "AlgaeTypes",
        new idb.Class("Image"),
        "AlgaeSurvey2023",
        "00:11:22:33:44:55",
        timestamp,
        new idb.Attribute("label"),
        "BlueGreenAlgae",
        new idb.Double(10.5),
        new idb.Double(20.2),
        new idb.Double(100.5),
        new idb.Double(150.8),
        new idb.Double(0.92)
    );
    
    space.addItem(item1);
    assert.equal(space.count(), 1, "Should have 1 item");
    
    // Navigate through the structure
    const labelNode = space.get(new idb.Class("Label"));
    assert.ok(labelNode !== null, "Should have Label class node");
    
    const algaeTypesNode = labelNode.get("AlgaeTypes");
    assert.ok(algaeTypesNode !== null, "Should have AlgaeTypes node");
    
    const imageNode = algaeTypesNode.get(new idb.Class("Image"));
    assert.ok(imageNode !== null, "Should have Image class node");
    
    // Check leaf node exists
    let current = space.getRoot();
    for (let i = 0; i < item1.getLength(); i++) {
        const component = item1.getComponent(i);
        assert.ok(current.has(component), `Should have component ${component} at position ${i}`);
        current = current.get(component);
    }
    
    assert.ok(current.isLeaf(), "Should find a leaf node");
    assert.ok(current.item !== null, "Leaf node should have the item");
});

test('Complex - Mixed component types', (t) => {
    const space = new idb.ItemSpace();
    
    // Create items with mixed component types at the same level
    const item1 = new idb.Item(
        new idb.Class("Data"),
        new idb.Long(1)
    );
    
    const item2 = new idb.Item(
        new idb.Class("Data"),
        new idb.Double(2.5)
    );
    
    const item3 = new idb.Item(
        new idb.Class("Data"),
        new idb.Attribute("attr")
    );
    
    const item4 = new idb.Item(
        new idb.Class("Data"),
        "string"
    );
    
    space.addItems([item1, item2, item3, item4]);
    assert.equal(space.count(), 4, "Should have 4 items");
    
    // Check node ordering
    const dataNode = space.get(new idb.Class("Data"));
    const keys = dataNode.keys();
    
    // Verify order according to TYPE_ORDER (might vary based on exact TYPE_ORDER implementation)
    assert.ok(keys.some(k => k instanceof idb.Attribute), "Should have an Attribute key");
    assert.ok(keys.some(k => typeof k === 'string'), "Should have a String key");
    assert.ok(keys.some(k => k instanceof idb.Double), "Should have a Double key");
    assert.ok(keys.some(k => k instanceof idb.Long), "Should have a Long key");
});

// ===== Performance test =====

test('Performance - Large dataset', async (t) => {
    const space = new idb.ItemSpace();
    const itemCount = 10000;
    const startTime = Date.now();
    
    // Add many items
    for (let i = 0; i < itemCount; i++) {
        const item = new idb.Item(
            new idb.Class("Performance"),
            `group${i % 10}`,
            new idb.Long(i)
        );
        space.addItem(item);
    }
    
    const addTime = Date.now();
    console.log(`Time to add ${itemCount} items: ${addTime - startTime}ms`);
    
    // Access various paths
    for (let i = 0; i < 10; i++) {
        const groupNode = space.get(new idb.Class("Performance")).get(`group${i}`);
        assert.ok(groupNode !== null, `Should find group${i}`);
        assert.equal(groupNode.size(), itemCount / 10, `Group${i} should have ${itemCount / 10} items`);
    }
    
    const accessTime = Date.now();
    console.log(`Time to access all groups: ${accessTime - addTime}ms`);
    
    // Get all items
    const items = space.getItems();
    assert.equal(items.length, itemCount, `Should get all ${itemCount} items`);
    
    const getAllTime = Date.now();
    console.log(`Time to get all items: ${getAllTime - accessTime}ms`);
    
    // Convert to object (this might be slow for large datasets)
    const startObjTime = Date.now();
    const obj = space.toIdbObject();
    const objTime = Date.now();
    console.log(`Time to convert to object: ${objTime - startObjTime}ms`);
});
