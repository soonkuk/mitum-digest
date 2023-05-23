package currency

import (
	"context"
	"github.com/ProtoconNet/mitum-currency/v2/base"
	types "github.com/ProtoconNet/mitum-currency/v2/operation/type"
	"github.com/ProtoconNet/mitum-currency/v2/state"
	"github.com/ProtoconNet/mitum-currency/v2/state/currency"
	"github.com/ProtoconNet/mitum-currency/v2/state/extension"
	"sync"

	mitumbase "github.com/ProtoconNet/mitum2/base"
	"github.com/ProtoconNet/mitum2/isaac"
	"github.com/ProtoconNet/mitum2/util"
	"github.com/pkg/errors"
)

var createAccountsItemProcessorPool = sync.Pool{
	New: func() interface{} {
		return new(CreateAccountsItemProcessor)
	},
}

var createAccountsProcessorPool = sync.Pool{
	New: func() interface{} {
		return new(CreateAccountsProcessor)
	},
}

func (CreateAccounts) Process(
	_ context.Context, _ mitumbase.GetStateFunc,
) ([]mitumbase.StateMergeValue, mitumbase.OperationProcessReasonError, error) {
	// NOTE Process is nil func
	return nil, nil, nil
}

type CreateAccountsItemProcessor struct {
	h    util.Hash
	item CreateAccountsItem
	ns   mitumbase.StateMergeValue
	nb   map[base.CurrencyID]mitumbase.StateMergeValue
}

func (opp *CreateAccountsItemProcessor) PreProcess(
	_ context.Context, _ mitumbase.Operation, getStateFunc mitumbase.GetStateFunc,
) error {
	e := util.StringErrorFunc("failed to preprocess for CreateAccountsItemProcessor")

	for i := range opp.item.Amounts() {
		am := opp.item.Amounts()[i]

		policy, err := state.ExistsCurrencyPolicy(am.Currency(), getStateFunc)
		if err != nil {
			return err
		}

		if am.Big().Compare(policy.NewAccountMinBalance()) < 0 {
			return mitumbase.NewBaseOperationProcessReasonError(
				"amount should be over minimum balance, %v < %v", am.Big(), policy.NewAccountMinBalance())
		}
	}

	target, err := opp.item.Address()
	if err != nil {
		return e(err, "")
	}

	st, err := state.NotExistsState(currency.StateKeyAccount(target), "keys of target", getStateFunc)
	if err != nil {
		return err
	}
	opp.ns = state.NewStateMergeValue(st.Key(), st.Value())

	nb := map[base.CurrencyID]mitumbase.StateMergeValue{}
	for i := range opp.item.Amounts() {
		am := opp.item.Amounts()[i]
		switch _, found, err := getStateFunc(currency.StateKeyBalance(target, am.Currency())); {
		case err != nil:
			return e(err, "")
		case found:
			return e(isaac.ErrStopProcessingRetry.Errorf("target balance already exists"), "")
		default:
			nb[am.Currency()] = state.NewStateMergeValue(currency.StateKeyBalance(target, am.Currency()), currency.NewBalanceStateValue(base.NewZeroAmount(am.Currency())))
		}
	}
	opp.nb = nb

	return nil
}

func (opp *CreateAccountsItemProcessor) Process(
	_ context.Context, _ mitumbase.Operation, _ mitumbase.GetStateFunc,
) ([]mitumbase.StateMergeValue, error) {
	e := util.StringErrorFunc("failed to preprocess for CreateAccountsItemProcessor")

	var (
		nac base.Account
		err error
	)

	if opp.item.AddressType() == base.EthAddressHint.Type() {
		nac, err = base.NewEthAccountFromKeys(opp.item.Keys())
	} else {
		nac, err = base.NewAccountFromKeys(opp.item.Keys())
	}
	if err != nil {
		return nil, e(err, "")
	}

	sts := make([]mitumbase.StateMergeValue, len(opp.item.Amounts())+1)
	sts[0] = state.NewStateMergeValue(opp.ns.Key(), currency.NewAccountStateValue(nac))

	for i := range opp.item.Amounts() {
		am := opp.item.Amounts()[i]
		v, ok := opp.nb[am.Currency()].Value().(currency.BalanceStateValue)
		if !ok {
			return nil, e(errors.Errorf("not BalanceStateValue, %T", opp.nb[am.Currency()].Value()), "")
		}
		stv := currency.NewBalanceStateValue(v.Amount.WithBig(v.Amount.Big().Add(am.Big())))
		sts[i+1] = state.NewStateMergeValue(opp.nb[am.Currency()].Key(), stv)
	}

	return sts, nil
}

func (opp *CreateAccountsItemProcessor) Close() {
	opp.h = nil
	opp.item = nil
	opp.ns = nil
	opp.nb = nil

	createAccountsItemProcessorPool.Put(opp)
}

type CreateAccountsProcessor struct {
	*mitumbase.BaseOperationProcessor
	ns       []*CreateAccountsItemProcessor
	required map[base.CurrencyID][2]base.Big // required[0] : amount + fee, required[1] : fee
}

func NewCreateAccountsProcessor() types.GetNewProcessor {
	return func(
		height mitumbase.Height,
		getStateFunc mitumbase.GetStateFunc,
		newPreProcessConstraintFunc mitumbase.NewOperationProcessorProcessFunc,
		newProcessConstraintFunc mitumbase.NewOperationProcessorProcessFunc,
	) (mitumbase.OperationProcessor, error) {
		e := util.StringErrorFunc("failed to create new CreateAccountsProcessor")

		nopp := createAccountsProcessorPool.Get()
		opp, ok := nopp.(*CreateAccountsProcessor)
		if !ok {
			return nil, errors.Errorf("expected CreateAccountsProcessor, not %T", nopp)
		}

		b, err := mitumbase.NewBaseOperationProcessor(
			height, getStateFunc, newPreProcessConstraintFunc, newProcessConstraintFunc)
		if err != nil {
			return nil, e(err, "")
		}

		opp.BaseOperationProcessor = b
		opp.ns = nil
		opp.required = nil

		return opp, nil
	}
}

func (opp *CreateAccountsProcessor) PreProcess(
	ctx context.Context, op mitumbase.Operation, getStateFunc mitumbase.GetStateFunc,
) (context.Context, mitumbase.OperationProcessReasonError, error) {
	fact, ok := op.Fact().(CreateAccountsFact)
	if !ok {
		return ctx, mitumbase.NewBaseOperationProcessReasonError("expected CreateAccountsFact, not %T", op.Fact()), nil
	}

	if err := state.CheckExistsState(currency.StateKeyAccount(fact.sender), getStateFunc); err != nil {
		return ctx, mitumbase.NewBaseOperationProcessReasonError("failed to check existence of sender %v : %w", fact.sender, err), nil
	}

	if err := state.CheckNotExistsState(extension.StateKeyContractAccount(fact.Sender()), getStateFunc); err != nil {
		return ctx, mitumbase.NewBaseOperationProcessReasonError("contract account cannot be create-account sender, %q: %w", fact.Sender(), err), nil
	}

	if err := state.CheckFactSignsByState(fact.sender, op.Signs(), getStateFunc); err != nil {
		return ctx, mitumbase.NewBaseOperationProcessReasonError("invalid signing :  %w", err), nil
	}

	for i := range fact.items {
		cip := createAccountsItemProcessorPool.Get()
		c, ok := cip.(*CreateAccountsItemProcessor)
		if !ok {
			return nil, mitumbase.NewBaseOperationProcessReasonError("expected CreateAccountsItemProcessor, not %T", cip), nil
		}

		c.h = op.Hash()
		c.item = fact.items[i]

		if err := c.PreProcess(ctx, op, getStateFunc); err != nil {
			return nil, mitumbase.NewBaseOperationProcessReasonError("fail to preprocess CreateAccountsItem: %w", err), nil
		}
	}

	return ctx, nil, nil
}

func (opp *CreateAccountsProcessor) Process( // nolint:dupl
	ctx context.Context, op mitumbase.Operation, getStateFunc mitumbase.GetStateFunc) (
	[]mitumbase.StateMergeValue, mitumbase.OperationProcessReasonError, error,
) {
	fact, ok := op.Fact().(CreateAccountsFact)
	if !ok {
		return nil, mitumbase.NewBaseOperationProcessReasonError("expected CreateAccountsFact, not %T", op.Fact()), nil
	}

	var (
		senderBalSts, feeReceiveBalSts map[base.CurrencyID]mitumbase.State
		required                       map[base.CurrencyID][2]base.Big
		err                            error
	)

	if feeReceiveBalSts, required, err = opp.calculateItemsFee(op, getStateFunc); err != nil {
		return nil, mitumbase.NewBaseOperationProcessReasonError("failed to calculate fee: %w", err), nil
	} else if senderBalSts, err = CheckEnoughBalance(fact.sender, required, getStateFunc); err != nil {
		return nil, mitumbase.NewBaseOperationProcessReasonError("not enough balance of sender %s : %w", fact.sender, err), nil
	} else {
		opp.required = required
	}

	ns := make([]*CreateAccountsItemProcessor, len(fact.items))
	for i := range fact.items {
		cip := createAccountsItemProcessorPool.Get()
		c, ok := cip.(*CreateAccountsItemProcessor)
		if !ok {
			return nil, mitumbase.NewBaseOperationProcessReasonError("expected CreateAccountsItemProcessor, not %T", cip), nil
		}

		c.h = op.Hash()
		c.item = fact.items[i]

		if err := c.PreProcess(ctx, op, getStateFunc); err != nil {
			return nil, mitumbase.NewBaseOperationProcessReasonError("fail to preprocess CreateAccountsItem: %w", err), nil
		}

		ns[i] = c
	}
	opp.ns = ns

	var stateMergeValues []mitumbase.StateMergeValue // nolint:prealloc
	for i := range opp.ns {
		s, err := opp.ns[i].Process(ctx, op, getStateFunc)
		if err != nil {
			return nil, mitumbase.NewBaseOperationProcessReasonError("failed to process CreateAccountsItem: %w", err), nil
		}
		stateMergeValues = append(stateMergeValues, s...)
	}

	for cid := range senderBalSts {
		v, ok := senderBalSts[cid].Value().(currency.BalanceStateValue)
		if !ok {
			return nil, mitumbase.NewBaseOperationProcessReasonError("expected BalanceStateValue, not %T", senderBalSts[cid].Value()), nil
		}

		var stateMergeValue mitumbase.StateMergeValue
		if senderBalSts[cid].Key() == feeReceiveBalSts[cid].Key() {
			stateMergeValue = state.NewStateMergeValue(
				senderBalSts[cid].Key(),
				currency.NewBalanceStateValue(v.Amount.WithBig(v.Amount.Big().Sub(opp.required[cid][0]).Add(opp.required[cid][1]))),
			)
		} else {
			stateMergeValue = state.NewStateMergeValue(
				senderBalSts[cid].Key(),
				currency.NewBalanceStateValue(v.Amount.WithBig(v.Amount.Big().Sub(opp.required[cid][0]))),
			)
			r, ok := feeReceiveBalSts[cid].Value().(currency.BalanceStateValue)
			if !ok {
				return nil, mitumbase.NewBaseOperationProcessReasonError("expected BalanceStateValue, not %T", feeReceiveBalSts[cid].Value()), nil
			}
			stateMergeValues = append(
				stateMergeValues,
				state.NewStateMergeValue(
					feeReceiveBalSts[cid].Key(),
					currency.NewBalanceStateValue(r.Amount.WithBig(r.Amount.Big().Add(opp.required[cid][1]))),
				),
			)
		}
		stateMergeValues = append(stateMergeValues, stateMergeValue)
	}

	return stateMergeValues, nil, nil
}

func (opp *CreateAccountsProcessor) Close() error {
	for i := range opp.ns {
		opp.ns[i].Close()
	}

	opp.ns = nil
	opp.required = nil

	createAccountsProcessorPool.Put(opp)

	return nil
}

func (opp *CreateAccountsProcessor) calculateItemsFee(
	op mitumbase.Operation,
	getStateFunc mitumbase.GetStateFunc,
) (map[base.CurrencyID]mitumbase.State, map[base.CurrencyID][2]base.Big, error) {
	fact, ok := op.Fact().(CreateAccountsFact)
	if !ok {
		return nil, nil, errors.Errorf("expected CreateAccountsFact, not %T", op.Fact())
	}

	items := make([]AmountsItem, len(fact.items))
	for i := range fact.items {
		items[i] = fact.items[i]
	}

	return CalculateItemsFee(getStateFunc, items)
}

func CalculateItemsFee(getStateFunc mitumbase.GetStateFunc, items []AmountsItem) (map[base.CurrencyID]mitumbase.State, map[base.CurrencyID][2]base.Big, error) {
	feeReceiveSts := map[base.CurrencyID]mitumbase.State{}
	required := map[base.CurrencyID][2]base.Big{}

	for i := range items {
		it := items[i]

		for j := range it.Amounts() {
			am := it.Amounts()[j]

			rq := [2]base.Big{base.ZeroBig, base.ZeroBig}
			if k, found := required[am.Currency()]; found {
				rq = k
			}

			policy, err := state.ExistsCurrencyPolicy(am.Currency(), getStateFunc)
			if err != nil {
				return nil, nil, err
			}

			var k base.Big
			switch k, err = policy.Feeer().Fee(am.Big()); {
			case err != nil:
				return nil, nil, err
			case !k.OverZero():
				required[am.Currency()] = [2]base.Big{rq[0].Add(am.Big()), rq[1]}
			default:
				required[am.Currency()] = [2]base.Big{rq[0].Add(am.Big()).Add(k), rq[1].Add(k)}
			}

			if policy.Feeer().Receiver() == nil {
				continue
			}

			if err := state.CheckExistsState(currency.StateKeyAccount(policy.Feeer().Receiver()), getStateFunc); err != nil {
				return nil, nil, err
			} else if st, found, err := getStateFunc(currency.StateKeyBalance(policy.Feeer().Receiver(), am.Currency())); err != nil {
				return nil, nil, err
			} else if !found {
				return nil, nil, errors.Errorf("feeer receiver %s not found", policy.Feeer().Receiver())
			} else {
				feeReceiveSts[am.Currency()] = st
			}
		}
	}

	return feeReceiveSts, required, nil
}

func CheckEnoughBalance(
	holder mitumbase.Address,
	required map[base.CurrencyID][2]base.Big,
	getStateFunc mitumbase.GetStateFunc,
) (map[base.CurrencyID]mitumbase.State, error) {
	sbSts := map[base.CurrencyID]mitumbase.State{}

	for cid := range required {
		rq := required[cid]

		st, err := state.ExistsState(currency.StateKeyBalance(holder, cid), "currency of holder", getStateFunc)
		if err != nil {
			return nil, err
		}

		am, err := currency.StateBalanceValue(st)
		if err != nil {
			return nil, mitumbase.NewBaseOperationProcessReasonError("insufficient balance of sender: %w", err)
		}

		if am.Big().Compare(rq[0]) < 0 {
			return nil, mitumbase.NewBaseOperationProcessReasonError(
				"insufficient balance of sender, %s; %d !> %d", holder.String(), am.Big(), rq[0])
		}
		sbSts[cid] = st
	}

	return sbSts, nil
}