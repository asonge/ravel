defmodule RavelCrdtPNCounterTest do
  use ExUnit.Case, async: true
  alias Ravel.CRDT
  alias Ravel.PNCounter
  
  setup do
      { :ok, [counter: PNCounter.new()] }
  end
  
  test "empty", context do
      assert 0 == PNCounter.value(context[:counter])
  end
  
  test "increments", context do
    counter = context[:counter]
    counter = PNCounter.increment(counter, :actor1)
    assert 1 == CRDT.value(counter)
    counter = PNCounter.increment(counter, :actor1, 5)
    assert 6 == CRDT.value(counter)
    counter = PNCounter.increment(counter, :actor2, 5)
    assert 11 == CRDT.value(counter)
  end
  
  test "decrements", context do
    counter = context[:counter]
    counter = PNCounter.decrement(counter, :actor1)
    assert -1 == CRDT.value(counter)
    counter = PNCounter.decrement(counter, :actor1, 5)
    assert -6 == CRDT.value(counter)
    counter = PNCounter.decrement(counter, :actor2, 5)
    assert -11 == CRDT.value(counter)
    counter = PNCounter.increment(counter, :actor2, 11)
    assert 0 == CRDT.value(counter)
  end
  
  test "simple merge", context do
    counter = context[:counter]
    counter = PNCounter.increment(counter, :actor1, 5)
    counter2 = PNCounter.decrement(counter, :actor2, 5)
    counter0 = CRDT.merge(counter, counter2)
    assert 0 == CRDT.value(counter0)
  end
  
  test "merge history", context do
    counter = context[:counter]
    counter1 = PNCounter.increment(counter, :actor1, 5)
    counter1 = PNCounter.decrement(counter1, :actor1, 5)
    counter2 = PNCounter.decrement(counter, :actor2, 5)
    counter0 = CRDT.merge(counter1, counter2)
    assert -5 == CRDT.value(counter0)
  end
  
  test "equal test", context do
    counter1 = context[:counter]
    counter2 = context[:counter]
    
    # Place these updates out-of-order, to make sure equals still works.
    counter1 = PNCounter.increment(counter1, :actor1, 5)
    counter1 = PNCounter.decrement(counter1, :actor2, 5)
    counter2 = PNCounter.decrement(counter2, :actor2, 5)
    counter2 = PNCounter.increment(counter2, :actor1, 5)
    
    assert true == PNCounter.equal(counter1, counter2)
    assert false == PNCounter.equal(context[:counter], counter1)
  end
  
  test "actors", context do
    counter = context[:counter]
    counter = PNCounter.increment(counter, :actor1, 5)
    counter = PNCounter.decrement(counter, :actor2, 5)
    assert [:actor1, :actor2] == CRDT.actors(counter)
  end
  
end
