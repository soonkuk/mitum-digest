package digest

import (
	"context"

	"github.com/ProtoconNet/mitum-currency/v3/digest/isaac"
	statecurrency "github.com/ProtoconNet/mitum-currency/v3/state/currency"
	stateextension "github.com/ProtoconNet/mitum-currency/v3/state/extension"
	"github.com/ProtoconNet/mitum2/base"
	mitumutil "github.com/ProtoconNet/mitum2/util"
	"go.mongodb.org/mongo-driver/mongo"
)

type BlockSessionPrepareFunc func(*BlockSession) error
type BlockSessionCommitFunc func(*BlockSession, context.Context, string, []mongo.WriteModel) error
type BlockSessionHandlerFunc func(*BlockSession, base.State) ([]mongo.WriteModel, error)

var (
	PrepareFuncs = map[string]BlockSessionPrepareFunc{
		"PrepareBlock":      PrepareBlock,
		"PrepareOperations": PrepareOperations,
		"PrepareCurrencies": PrepareCurrencies,
		"PrepareAccounts":   PrepareAccounts,
	}
	HandlerFuncs = map[string]BlockSessionHandlerFunc{
		"HandleAccountState":         HandleAccountState,
		"HandleBalanceState":         HandleBalanceState,
		"HandleContractAccountState": HandleContractAccountState,
		"HandleCurrencyState":        HandleCurrencyState,
	}
	CommitFuncs = map[string]BlockSessionCommitFunc{
		defaultColNameBlock:           CommitFunc,
		defaultColNameOperation:       CommitFunc,
		defaultColNameAccount:         CommitFunc,
		defaultColNameContractAccount: CommitFunc,
		defaultColNameBalance:         CommitFunc,
		defaultColNameCurrency:        CommitFunc,
	}
)

func CommitFunc(bs *BlockSession, ctx context.Context, colName string, writeModels []mongo.WriteModel) error {
	if len(writeModels) > 0 {
		if err := bs.WriteWriteModels(ctx, colName, writeModels); err != nil {
			return err
		}
	}

	return nil
}

func PrepareBlock(bs *BlockSession) error {
	if bs.block == nil {
		return nil
	}

	blockModels := make([]mongo.WriteModel, 1)

	manifest := isaac.NewManifest(
		bs.block.Manifest().Height(),
		bs.block.Manifest().Previous(),
		bs.block.Manifest().Proposal(),
		bs.block.Manifest().OperationsTree(),
		bs.block.Manifest().StatesTree(),
		bs.block.Manifest().Suffrage(),
		bs.block.Manifest().ProposedAt(),
	)

	doc, err := NewManifestDoc(
		manifest,
		bs.st.database.Encoder(),
		bs.block.Manifest().Height(),
		bs.ops,
		bs.block.SignedAt(),
		bs.proposal.ProposalFact().Proposer(),
		bs.proposal.ProposalFact().Point().Round(),
	)
	if err != nil {
		return err
	}
	blockModels[0] = mongo.NewInsertOneModel().SetDocument(doc)

	return bs.SetWriteModel(defaultColNameBlock, blockModels)
}

func PrepareOperations(bs *BlockSession) error {
	if len(bs.ops) < 1 {
		return nil
	}

	node := func(h mitumutil.Hash) (bool, bool, base.OperationProcessReasonError) {
		no, found := bs.opsTreeNodes[h.String()]
		if !found {
			return false, false, nil
		}

		return true, no.InState(), no.Reason()
	}

	operationModels := make([]mongo.WriteModel, len(bs.ops))

	for i := range bs.ops {
		op := bs.ops[i]

		var doc OperationDoc
		switch found, inState, reason := node(op.Fact().Hash()); {
		case !found:
			return mitumutil.ErrNotFound.Errorf("operation, %v in operations tree", op.Fact().Hash().String())
		default:
			var reasonMsg string
			switch {
			case reason == nil:
				reasonMsg = ""
			default:
				reasonMsg = reason.Msg()
			}
			d, err := NewOperationDoc(
				op,
				bs.st.database.Encoder(),
				bs.block.Manifest().Height(),
				bs.block.SignedAt(),
				inState,
				reasonMsg,
				uint64(i),
			)
			if err != nil {
				return err
			}
			doc = d
		}

		operationModels[i] = mongo.NewInsertOneModel().SetDocument(doc)
	}

	return bs.SetWriteModel(defaultColNameOperation, operationModels)
}

func PrepareAccounts(bs *BlockSession) error {
	if len(bs.sts) < 1 {
		return nil
	}

	var accountModels []mongo.WriteModel
	var balanceModels []mongo.WriteModel
	var contractAccountModels []mongo.WriteModel
	for i := range bs.sts {
		st := bs.sts[i]

		switch {
		case statecurrency.IsStateAccountKey(st.Key()):
			j, err := bs.HandlerFuncs["HandleAccountState"](bs, st)
			if err != nil {
				return err
			}
			accountModels = append(accountModels, j...)
		case statecurrency.IsStateBalanceKey(st.Key()):
			j, err := bs.HandlerFuncs["HandleBalanceState"](bs, st)
			if err != nil {
				return err
			}
			balanceModels = append(balanceModels, j...)
		case stateextension.IsStateContractAccountKey(st.Key()):
			j, err := bs.HandlerFuncs["HandleContractAccountState"](bs, st)
			if err != nil {
				return err
			}
			contractAccountModels = append(contractAccountModels, j...)
		default:
			continue
		}
	}

	err := bs.SetWriteModel(defaultColNameAccount, accountModels)
	if err != nil {
		return err
	}
	err = bs.SetWriteModel(defaultColNameContractAccount, contractAccountModels)
	if err != nil {
		return err
	}
	return bs.SetWriteModel(defaultColNameBalance, balanceModels)
}

func PrepareCurrencies(bs *BlockSession) error {
	if len(bs.sts) < 1 {
		return nil
	}

	var currencyModels []mongo.WriteModel
	for i := range bs.sts {
		st := bs.sts[i]
		switch {
		case statecurrency.IsStateCurrencyDesignKey(st.Key()):
			j, err := bs.HandlerFuncs["HandleCurrencyState"](bs, st)
			if err != nil {
				return err
			}
			currencyModels = append(currencyModels, j...)
		default:
			continue
		}
	}

	return bs.SetWriteModel(defaultColNameCurrency, currencyModels)
}

func HandleAccountState(bs *BlockSession, st base.State) ([]mongo.WriteModel, error) {
	if rs, err := NewAccountValue(st); err != nil {
		return nil, err
	} else if doc, err := NewAccountDoc(rs, bs.st.database.Encoder()); err != nil {
		return nil, err
	} else {
		return []mongo.WriteModel{mongo.NewInsertOneModel().SetDocument(doc)}, nil
	}
}

func HandleBalanceState(bs *BlockSession, st base.State) ([]mongo.WriteModel, error) {
	doc, err := NewBalanceDoc(st, bs.st.database.Encoder())
	if err != nil {
		return nil, err
	}
	return []mongo.WriteModel{mongo.NewInsertOneModel().SetDocument(doc)}, nil
}

func HandleContractAccountState(bs *BlockSession, st base.State) ([]mongo.WriteModel, error) {
	doc, err := NewContractAccountStatusDoc(st, bs.st.database.Encoder())
	if err != nil {
		return nil, err
	}
	return []mongo.WriteModel{mongo.NewInsertOneModel().SetDocument(doc)}, nil
}

func HandleCurrencyState(bs *BlockSession, st base.State) ([]mongo.WriteModel, error) {
	doc, err := NewCurrencyDoc(st, bs.st.database.Encoder())
	if err != nil {
		return nil, err
	}
	return []mongo.WriteModel{mongo.NewInsertOneModel().SetDocument(doc)}, nil
}
