package digest

import (
	mongodbstorage "github.com/ProtoconNet/mitum-currency/v3/digest/mongodb"
	bsonenc "github.com/ProtoconNet/mitum-currency/v3/digest/util/bson"
	"github.com/ProtoconNet/mitum-currency/v3/state/currency"
	"github.com/ProtoconNet/mitum-currency/v3/state/extension"
	"github.com/ProtoconNet/mitum-currency/v3/types"
	"github.com/ProtoconNet/mitum2/base"
	"github.com/ProtoconNet/mitum2/util/encoder"
	"github.com/pkg/errors"
)

type AccountDoc struct {
	mongodbstorage.BaseDoc
	address string
	height  base.Height
	pubs    []string
}

func NewAccountDoc(rs AccountValue, enc encoder.Encoder) (AccountDoc, error) {
	b, err := mongodbstorage.NewBaseDoc(nil, rs, enc)
	if err != nil {
		return AccountDoc{}, err
	}

	var pubs []string
	if keys := rs.Account().Keys(); keys != nil {
		ks := keys.Keys()
		pubs = make([]string, len(ks))
		for i := range ks {
			k := ks[i].Key()
			pubs[i] = k.String()
		}
	}

	address := rs.ac.Address()
	return AccountDoc{
		BaseDoc: b,
		address: address.String(),
		height:  rs.height,
		pubs:    pubs,
	}, nil
}

func (doc AccountDoc) MarshalBSON() ([]byte, error) {
	m, err := doc.BaseDoc.M()
	if err != nil {
		return nil, err
	}

	m["address"] = doc.address
	m["height"] = doc.height
	m["pubs"] = doc.pubs

	return bsonenc.Marshal(m)
}

type BalanceDoc struct {
	mongodbstorage.BaseDoc
	st base.State
	am types.Amount
}

// NewBalanceDoc gets the State of Amount
func NewBalanceDoc(st base.State, enc encoder.Encoder) (BalanceDoc, error) {
	am, err := currency.StateBalanceValue(st)
	if err != nil {
		return BalanceDoc{}, errors.Wrap(err, "BalanceDoc needs Amount state")
	}

	b, err := mongodbstorage.NewBaseDoc(nil, st, enc)
	if err != nil {
		return BalanceDoc{}, err
	}

	return BalanceDoc{
		BaseDoc: b,
		st:      st,
		am:      am,
	}, nil
}

func (doc BalanceDoc) MarshalBSON() ([]byte, error) {
	m, err := doc.BaseDoc.M()
	if err != nil {
		return nil, err
	}

	address := doc.st.Key()[:len(doc.st.Key())-len(currency.StateKeyBalanceSuffix)-len(doc.am.Currency())-1]
	m["address"] = address
	m["currency"] = doc.am.Currency().String()
	m["height"] = doc.st.Height()
	m["amount"] = doc.am.Big().String()

	return bsonenc.Marshal(m)
}

type ContractAccountStatusDoc struct {
	mongodbstorage.BaseDoc
	st  base.State
	cas types.ContractAccountStatus
}

func NewContractAccountStatusDoc(st base.State, enc encoder.Encoder) (ContractAccountStatusDoc, error) {
	cd, err := extension.StateContractAccountValue(st)
	if err != nil {
		return ContractAccountStatusDoc{}, errors.Wrap(err, "ContractAccountStatusDoc needs ContractAccountStatus state")
	}

	b, err := mongodbstorage.NewBaseDoc(nil, st, enc)
	if err != nil {
		return ContractAccountStatusDoc{}, err
	}

	return ContractAccountStatusDoc{
		BaseDoc: b,
		st:      st,
		cas:     cd,
	}, nil
}

func (doc ContractAccountStatusDoc) MarshalBSON() ([]byte, error) {
	m, err := doc.BaseDoc.M()
	if err != nil {
		return nil, err
	}

	address := doc.st.Key()[:len(doc.st.Key())-len(extension.StateKeyContractAccountSuffix)]
	m["address"] = address
	m["owner"] = doc.cas.Owner().String()
	m["height"] = doc.st.Height()
	m["contract"] = true

	return bsonenc.Marshal(m)
}
