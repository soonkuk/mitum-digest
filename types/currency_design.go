package types

import (
	"github.com/ProtoconNet/mitum-currency/v3/common"
	"github.com/ProtoconNet/mitum2/base"
	"github.com/ProtoconNet/mitum2/util"
	"github.com/ProtoconNet/mitum2/util/hint"
	"github.com/pkg/errors"
)

var (
	CurrencyDesignHint = hint.MustNewHint("mitum-currency-currency-design-v0.0.1")
)

type CurrencyDesign struct {
	hint.BaseHinter
	amount         Amount
	genesisAccount base.Address
	policy         CurrencyPolicy
	aggregate      common.Big
}

func NewCurrencyDesign(amount Amount, genesisAccount base.Address, po CurrencyPolicy) CurrencyDesign {
	return CurrencyDesign{
		BaseHinter:     hint.NewBaseHinter(CurrencyDesignHint),
		amount:         amount,
		genesisAccount: genesisAccount,
		policy:         po,
		aggregate:      amount.Big(),
	}
}

func (de CurrencyDesign) IsValid([]byte) error {
	if err := util.CheckIsValiders(nil, false,
		de.BaseHinter,
		de.amount,
		de.aggregate,
	); err != nil {
		return util.ErrInvalid.Errorf("invalid currency balance: %v", err)
	}

	switch {
	case !de.amount.Big().OverZero():
		return util.ErrInvalid.Errorf("currency balance should be over zero")
	case !de.aggregate.OverZero():
		return util.ErrInvalid.Errorf("aggregate should be over zero")
	}

	if de.genesisAccount != nil {
		if err := de.genesisAccount.IsValid(nil); err != nil {
			return util.ErrInvalid.Errorf("invalid CurrencyDesign: %v", err)
		}
	}

	if err := de.policy.IsValid(nil); err != nil {
		return util.ErrInvalid.Errorf("invalid CurrencyPolicy: %v", err)
	}

	return nil
}

func (de CurrencyDesign) Bytes() []byte {
	var gb []byte
	if de.genesisAccount != nil {
		gb = de.genesisAccount.Bytes()
	}

	return util.ConcatBytesSlice(
		de.amount.Bytes(),
		gb,
		de.policy.Bytes(),
		de.aggregate.Bytes(),
	)
}

func (de CurrencyDesign) GenesisAccount() base.Address {
	return de.genesisAccount
}

func (de CurrencyDesign) Amount() Amount {
	return de.amount
}

func (de CurrencyDesign) Currency() CurrencyID {
	return de.amount.cid
}

func (de CurrencyDesign) Policy() CurrencyPolicy {
	return de.policy
}

func (de *CurrencyDesign) SetGenesisAccount(ac base.Address) {
	de.genesisAccount = ac
}

func (de *CurrencyDesign) SetPolicy(po CurrencyPolicy) {
	de.policy = po
}

func (de CurrencyDesign) Aggregate() common.Big {
	return de.aggregate
}

func (de CurrencyDesign) AddAggregate(b common.Big) (CurrencyDesign, error) {
	if !b.OverZero() {
		return de, errors.Errorf("new aggregate not over zero")
	}

	de.aggregate = de.aggregate.Add(b)

	return de, nil
}
