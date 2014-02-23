defmodule RavelGCounterTest do
  use ExUnit.Case , async: true
  
  alias Ravel.CRDT
  alias Ravel.GCounter
  
  setup do
    { :ok, [counter: GCounter.new()] }
  end
  
  test "empty", context do
      assert 0 == GCounter.value(context[:counter])
  end
  
  test "increments", context do
    counter = context[:counter]
    counter = GCounter.increment(counter, :actor1)
    assert 1 == CRDT.value(counter)
    counter = GCounter.increment(counter, :actor1, 5)
    assert 6 == CRDT.value(counter)
    counter = GCounter.increment(counter, :actor2, 5)
    assert 11 == CRDT.value(counter)
  end
  
  test "simple merge", context do
    counter = context[:counter]
    counter = GCounter.increment(counter, :actor1, 5)
    counter2 = GCounter.increment(counter, :actor2, 5)
    counter0 = CRDT.merge(counter, counter2)
    assert 10 == CRDT.value(counter0)
  end
  
  test "merge history", context do
    counter = context[:counter]
    counter1 = GCounter.increment(counter, :actor1, 5)
    counter1 = GCounter.increment(counter1, :actor2, 5)
    counter2 = GCounter.increment(counter, :actor2, 5)
    counter0 = CRDT.merge(counter1, counter2)
    assert 10 == CRDT.value(counter0)
  end
  
  test "equal test", context do
    counter1 = context[:counter]
    counter2 = context[:counter]
    
    # Place these updates out-of-order, to make sure equals still works.
    counter1 = GCounter.increment(counter1, :actor1, 5)
    counter1 = GCounter.increment(counter1, :actor2, 5)
    counter2 = GCounter.increment(counter2, :actor2, 5)
    counter2 = GCounter.increment(counter2, :actor1, 5)
    
    assert true == CRDT.equal?(counter1, counter2)
    # IO.inspect(context[:counter], pretty: false)
    # IO.inspect(counter1, pretty: false)
    # IO.inspect(GCounter.equal?(context[:counter], counter1))
    assert false == CRDT.equal?(context[:counter], counter1)
  end
  
  test "actors", context do
    counter = context[:counter]
    counter = GCounter.increment(counter, :actor1, 5)
    counter = GCounter.increment(counter, :actor2, 5)
    assert [:actor1, :actor2] == CRDT.actors(counter)
  end
  
end
