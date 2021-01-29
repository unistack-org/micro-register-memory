package memory

import (
	"testing"

	"github.com/unistack-org/micro/v3/register"
)

func TestWatcher(t *testing.T) {
	w := &Watcher{
		id:   "test",
		res:  make(chan *register.Result),
		exit: make(chan bool),
		wo: register.WatchOptions{
			Domain: register.WildcardDomain,
		},
	}

	go func() {
		w.res <- &register.Result{
			Service: &register.Service{Name: "foo"},
		}
	}()

	_, err := w.Next()
	if err != nil {
		t.Fatal("unexpected err", err)
	}

	w.Stop()

	if _, err := w.Next(); err == nil {
		t.Fatal("expected error on Next()")
	}
}
