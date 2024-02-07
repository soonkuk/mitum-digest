package digest

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/pkg/errors"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"

	"github.com/ProtoconNet/mitum2/base"
	"github.com/ProtoconNet/mitum2/util/fixedtree"
)

var bulkWriteLimit = 500

type BlockSessioner interface {
	Prepare() error
	Commit(context.Context) error
	Close() error
}

type BlockSession struct {
	sync.RWMutex
	block        base.BlockMap
	ops          []base.Operation
	opsTree      fixedtree.Tree
	sts          []base.State
	st           *Database
	proposal     base.ProposalSignFact
	opsTreeNodes map[string]base.OperationFixedtreeNode
	WriteModels  map[string][]mongo.WriteModel
	PrepareFuncs map[string]BlockSessionPrepareFunc
	HandlerFuncs map[string]BlockSessionHandlerFunc
	CommitFuncs  map[string]BlockSessionCommitFunc
	StatesValue  *sync.Map
}

func NewBlockSession() *BlockSession {
	return &BlockSession{
		WriteModels:  make(map[string][]mongo.WriteModel),
		PrepareFuncs: make(map[string]BlockSessionPrepareFunc),
		HandlerFuncs: make(map[string]BlockSessionHandlerFunc),
		CommitFuncs:  make(map[string]BlockSessionCommitFunc),
		StatesValue:  &sync.Map{},
	}
}

func (bs *BlockSession) SetPrepareFuncs(prepareFuncs map[string]BlockSessionPrepareFunc) {
	for k, f := range prepareFuncs {
		_, found := bs.PrepareFuncs[k]
		if !found {
			bs.PrepareFuncs[k] = f
		}
	}
}

func (bs *BlockSession) SetHandlerFuncs(handlerFuncs map[string]BlockSessionHandlerFunc) {
	for k, f := range handlerFuncs {
		_, found := bs.HandlerFuncs[k]
		if !found {
			bs.HandlerFuncs[k] = f
		}
	}
}

func (bs *BlockSession) SetCommitFuncs(commitFuncs map[string]BlockSessionCommitFunc) {
	for k, f := range commitFuncs {
		_, found := bs.CommitFuncs[k]
		if !found {
			bs.CommitFuncs[k] = f
		}
	}
}

func (bs *BlockSession) SetWriteModel(modelName string, m []mongo.WriteModel) error {
	_, found := bs.WriteModels[modelName]
	if found {
		return errors.Errorf("%s Writemodel is already registered", modelName)
	}

	bs.WriteModels[modelName] = m
	return nil
}

func (bs *BlockSession) Prepare() error {
	bs.Lock()
	defer bs.Unlock()

	if err := bs.prepareOperationsTree(); err != nil {
		return err
	}

	for _, f := range bs.PrepareFuncs {
		err := f(bs)
		if err != nil {
			return err
		}
	}

	return nil
}

func (bs *BlockSession) Database() *Database {
	return bs.st
}

func (bs *BlockSession) States() []base.State {
	return bs.sts
}

func (bs *BlockSession) BLock() base.BlockMap {
	return bs.block
}

func (bs *BlockSession) Commit(ctx context.Context, pool *sync.Pool) error {
	bs.Lock()
	defer bs.Unlock()

	started := time.Now()
	defer func() {
		bs.StatesValue.Store("commit", time.Since(started))

		_ = bs.close(pool)
	}()

	for colName, writeModels := range bs.WriteModels {
		f, found := bs.CommitFuncs[colName]
		if !found {
			return errors.Errorf("BlockSessionCommitFunc not found for %s", colName)
		}

		err := f(bs, ctx, colName, writeModels)
		if err != nil {
			return err
		}
	}

	return nil
}

func (bs *BlockSession) Close(pool *sync.Pool) error {
	bs.Lock()
	defer bs.Unlock()

	return bs.close(pool)
}

func (bs *BlockSession) prepareOperationsTree() error {
	nodes := map[string]base.OperationFixedtreeNode{}

	if err := bs.opsTree.Traverse(func(_ uint64, no fixedtree.Node) (bool, error) {
		nno := no.(base.OperationFixedtreeNode)
		if nno.InState() {
			nodes[nno.Key()] = nno
		} else {
			nodes[nno.Key()[:len(nno.Key())-1]] = nno
		}

		return true, nil
	}); err != nil {
		return err
	}

	bs.opsTreeNodes = nodes

	return nil
}

func (bs *BlockSession) WriteWriteModels(ctx context.Context, col string, models []mongo.WriteModel) error {
	started := time.Now()
	defer func() {
		bs.StatesValue.Store(fmt.Sprintf("write-models-%s", col), time.Since(started))
	}()

	n := len(models)
	if n < 1 {
		return nil
	} else if n <= bulkWriteLimit {
		return bs.writeModelsChunk(ctx, col, models)
	}

	z := n / bulkWriteLimit
	if n%bulkWriteLimit != 0 {
		z++
	}

	for i := 0; i < z; i++ {
		s := i * bulkWriteLimit
		e := s + bulkWriteLimit
		if e > n {
			e = n
		}

		if err := bs.writeModelsChunk(ctx, col, models[s:e]); err != nil {
			return err
		}
	}

	return nil
}

func (bs *BlockSession) writeModelsChunk(ctx context.Context, col string, models []mongo.WriteModel) error {
	opts := options.BulkWrite().SetOrdered(false)
	if res, err := bs.st.database.Client().Collection(col).BulkWrite(ctx, models, opts); err != nil {
		return err
	} else if res != nil && res.InsertedCount < 1 {
		return errors.Errorf("not inserted to %s", col)
	}

	return nil
}

func (bs *BlockSession) close(pool *sync.Pool) error {
	bs.block = nil
	bs.WriteModels = make(map[string][]mongo.WriteModel)
	err := bs.st.Close()
	if err != nil {
		return err
	}

	pool.Put(bs)

	return nil
}
