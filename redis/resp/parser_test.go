package parser

import (
	"bytes"
	"testing"
)

func TestRESP(t *testing.T) {
	strTest := []byte("+OK\r\n")
	errTest := []byte("-ERR Invalid Synatx\r\n")
	intTest1 := []byte(":10010\r\n")
	intTest2 := []byte(":-10086\r\n")
	bstrTest1 := []byte("$-1\r\n")
	bstrTest2 := []byte("$0\r\n\r\n")
	bstrTest3 := []byte("$3\r\nSET\r\n")
	arrayTest := []byte("*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n")

	testData := append(strTest, errTest...)
	testData = append(testData, intTest1...)
	testData = append(testData, intTest2...)
	testData = append(testData, bstrTest1...)
	testData = append(testData, bstrTest2...)
	testData = append(testData, bstrTest3...)
	testData = append(testData, arrayTest...)

	testCount := 8
	results := make([]*Payload, 0)
	rawreader := bytes.NewReader(testData)
	ch := ParseStream(rawreader)
	count := 0
	for payload := range ch {
		if payload.Err != nil {
			t.Log(payload.Err)
		}
		results = append(results, payload)
		count++
		if count == testCount {
			break
		}
	}

	// Test Simple String type
	if v, ok := results[0].Data.(*String); !ok {
		t.Log("Fail to parse simple string type")
		t.Fail()
	} else if v.Arg != "OK" {
		t.Logf("Wrong parsed result of simple string: %s", v.Arg)
		t.Fail()
	} else if !bytes.Equal(v.Serialize(), strTest) {
		t.Logf("Serialized simple string wrongly.\nParsed: %v\nShould be:%v\n", results[0].Data.Serialize(), strTest)
		t.Fail()
	}

	// Test Error type
	if v, ok := results[1].Data.(*Error); !ok {
		t.Log("Fail to parse Error type")
		t.Fail()
	} else if v.Arg != "ERR Invalid Synatx" {
		t.Logf("Wrong parsed result of Error: %s", v.Arg)
		t.Fail()
	} else if !bytes.Equal(v.Serialize(), errTest) {
		t.Logf("Serialized error wrongly.\nParsed: %v\nShould be:%v\n", results[1].Data.Serialize(), errTest)
		t.Fail()
	}

	// Test Integer type
	if v, ok := results[2].Data.(*Integer); !ok {
		t.Log("Fail to parse Integer type")
		t.Fail()
	} else if v.Arg != 10010 {
		t.Logf("Wrong parsed result of Integer: %v", v.Arg)
		t.Fail()
	} else if !bytes.Equal(v.Serialize(), intTest1) {
		t.Logf("Serialized Integer wrongly.\nParsed: %v\nShould be:%v\n", results[2].Data.Serialize(), intTest1)
		t.Fail()
	}
	if v, ok := results[3].Data.(*Integer); !ok {
		t.Log("Fail to parse Integer type")
		t.Fail()
	} else if v.Arg != -10086 {
		t.Logf("Wrong parsed result of Integer: %v", v.Arg)
		t.Fail()
	} else if !bytes.Equal(v.Serialize(), intTest2) {
		t.Logf("Serialized Integer wrongly.\nParsed: %v\nShould be:%v\n", results[3].Data.Serialize(), intTest2)
		t.Fail()
	}

	// Test BulkString type
	if v, ok := results[4].Data.(*BulkString); !ok {
		t.Log("Fail to parse BulkString type")
		t.Fail()
	} else if v.Arg != nil {
		t.Logf("Wrong parsed result of BulkString: %s", v.Arg)
		t.Fail()
	} else if !bytes.Equal(v.Serialize(), bstrTest1) {
		t.Logf("Serialized BulkString wrongly.\nParsed: %v\nShould be:%v\n", results[4].Data.Serialize(), bstrTest1)
		t.Fail()
	}
	if v, ok := results[5].Data.(*BulkString); !ok {
		t.Log("Fail to parse BulkString type")
		t.Fail()
	} else if len(v.Arg) != 0 || v.Arg == nil {
		t.Logf("Wrong parsed result of BulkString: %s", v.Arg)
		t.Fail()
	}else if !bytes.Equal(v.Serialize(), bstrTest2) {
		t.Logf("Serialized BulkString wrongly.\nParsed: %v\nShould be:%v\n", results[5].Data.Serialize(), bstrTest2)
		t.Fail()
	}
	if v, ok := results[6].Data.(*BulkString); !ok {
		t.Log("Fail to parse BulkString type")
		t.Fail()
	} else if !bytes.Equal(v.Arg, []byte("SET")) {
		t.Logf("Wrong parsed result of BulkString: %s", v.Arg)
		t.Fail()
	}else if !bytes.Equal(v.Serialize(), bstrTest3) {
		t.Logf("Serialized BulkString wrongly.\nParsed: %v\nShould be:%v\n", results[6].Data.Serialize(), bstrTest3)
		t.Fail()
	}

	// Test Array type
	if v, ok := results[7].Data.(*Array); !ok {
		t.Log("Fail to parse Array type")
		t.Fail()
	} else if correct := bytes.Equal(v.Args[0], []byte("SET")) && 
				   		 bytes.Equal(v.Args[1], []byte("key")) &&
				   		 bytes.Equal(v.Args[2], []byte("value")); !correct {
			t.Logf("Wrong parsed result of BulkString: %v", v.Args)
			t.Fail()
	} else if !bytes.Equal(v.Serialize(), arrayTest) {
		t.Logf("Serialized BulkString wrongly.\nParsed: %v\nShould be:%v\n", results[7].Data.Serialize(), arrayTest)
		t.Fail()
	}
}