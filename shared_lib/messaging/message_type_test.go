package messaging

import "testing"

func TestMsgTypeOrdinalsMatchMLContract(t *testing.T) {
	expect := map[MsgType]int{
		MsgTypeUnknown:                    0,
		MsgTypeUserCreated:                1,
		MsgTypeUserUpdated:                2,
		MsgTypeUserDeleted:                3,
		MsgTypeEmailVerificationRequested: 4,
		MsgTypeDatasetFileUploaded:        5,
	}

	for msgType, ordinal := range expect {
		if got := int(msgType); got != ordinal {
			t.Fatalf("%s ordinal changed: got %d want %d", msgType.String(), got, ordinal)
		}
	}
}

func TestMsgTypeStringMappingsRoundTrip(t *testing.T) {
	for _, msgType := range []MsgType{
		MsgTypeUserCreated,
		MsgTypeUserUpdated,
		MsgTypeUserDeleted,
		MsgTypeEmailVerificationRequested,
		MsgTypeDatasetFileUploaded,
	} {
		if msgType.String() == "" {
			t.Fatalf("missing string mapping for msg type ordinal %d", msgType)
		}
		if MsgTypeFromString(msgType.String()) != msgType {
			t.Fatalf("msg type %s does not round-trip from string", msgType.String())
		}
	}
}
