package gocbcore

import "testing"

func (suite *UnitTestSuite) TestCommonFlags() {
	type tCase struct {
		name        string
		valueType   DataType
		compression CompressionType
	}

	testCases := []tCase{
		{
			name:        "must return json type with unknown compression",
			valueType:   JSONType,
			compression: UnknownCompression,
		},
		{
			name:        "must return json type with no compression",
			valueType:   JSONType,
			compression: NoCompression,
		},
		{
			name:        "must return binary type with unknown compression",
			valueType:   BinaryType,
			compression: UnknownCompression,
		},
		{
			name:        "must return binary type with no compression",
			valueType:   BinaryType,
			compression: NoCompression,
		},
		{
			name:        "must return string type with unknown compression",
			valueType:   StringType,
			compression: UnknownCompression,
		},
		{
			name:        "must return string type with no compression",
			valueType:   StringType,
			compression: NoCompression,
		},
		//
		// {
		// 	name:        "must return unknown type with unknown compression",
		// 	valueType:   UnknownType,
		// 	compression: UnknownCompression,
		// },
		// {
		// 	name:        "must return unknown type with no compression",
		// 	valueType:   UnknownType,
		// 	compression: NoCompression,
		// },
	}

	for _, tCase := range testCases {
		suite.T().Run(tCase.name, func(te *testing.T) {
			flags := EncodeCommonFlags(tCase.valueType, tCase.compression)

			valueType, compression := DecodeCommonFlags(flags)

			if tCase.valueType != valueType {
				te.Errorf("wrong valueType (expects %v, got %v)", tCase.valueType, valueType)
			}

			if tCase.compression != compression {
				te.Errorf("wrong compression (expects %v, got %v)", tCase.compression, compression)
			}
		})
	}
}

// func TestCommonFlags(t *testing.T) {
// 	t.Parallel()

// 	t.Run("issue #10 unknown flag ignored", func(t *testing.T) {
// 		t.Parallel()

// 		flags := gocbcore.EncodeCommonFlags(gocbcore.JSONType, gocbcore.UnknownCompression)

// 		_, compression := gocbcore.DecodeCommonFlags(flags)

// 		ass
// 	})

// }
