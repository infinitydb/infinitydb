import {idb} from "./infinitydb_access.js";
import test from 'node:test';
import assert from 'node:assert';

let INTERFACE = "examples";
let server = new idb.Accessor("https://infinitydb.com:37411/infinitydb/data", "demo/writeable", "testUser", "db");


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
    let [success, result, content_type] = await server.get_json([AIRCRAFT, new idb.Long(737)]);
    assert(success);
    assert.equal(content_type, "application/json");
})
