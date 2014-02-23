
defmodule RavelORDictTest do
  use ExUnit.Case, async: true
  use ExCheck
  alias Ravel.CRDT
  alias Ravel.GCounter
  alias Ravel.ORDict
  alias Ravel.PNCounter
  
  setup do
    gc = GCounter.new()
    a  = GCounter.increment(gc, :a)
    b  = GCounter.increment(gc, :a, 2)
    c  = GCounter.increment(gc, :a, 3)
    b1 = GCounter.increment(gc, :b, 2)
    c1 = GCounter.increment(gc, :b, 3)
    d  = GCounter.increment(gc, :b)
    e  = GCounter.increment(gc, :b)
    f  = GCounter.increment(gc, :b)
    {:ok, [
      a: a, b: b, c: c, b1: b1, c1: c1, d: d, e: e, f: f,
      empty: ORDict.new,
      a_abc: ORDict.new([a: a, b: b, c: c], [actor: :a]),
      b_abc: ORDict.new([a: a, b: b, c: c], [actor: :b]),
      b_bcd: ORDict.new([b: b1, c: c1, d: d], [actor: :b]),
      b_def: ORDict.new([d: d, e: e, f: f], [actor: :b]),
      b_456: ORDict.new([d: d, e: e, f: f], [actor: :b])
    ]}
  end
  
  test "basic values", ctx do
    dictlist = Keyword.take(ctx, [:a, :b, :c])
    assert [] == CRDT.value(ctx[:empty])
    assert dictlist == CRDT.value(ctx[:a_abc]) |> Enum.sort
    assert CRDT.equal_value?(ctx[:a_abc], ctx[:b_abc])
    assert ctx[:empty] == ORDict.empty(ctx[:a_abc])
  end
  
  test "new equivalence", ctx do
    dictlist = Keyword.take(ctx, [:a, :b, :c])
    assert dictlist == ORDict.new(Enum.map(dictlist, &{:a, &1})) |> sort_value
    assert dictlist == ORDict.new(dictlist, &{:a, &1}) |> sort_value
    assert dictlist == ORDict.new(dictlist, &{:a, &1}) |> ORDict.to_list |> Enum.sort
  end
  
  test "actors", ctx do
    assert [:a] == ORDict.actors(ctx[:a_abc])
    assert [:b] == ORDict.actors(ctx[:b_bcd])
    assert [:a,:b] == ORDict.actors(ORDict.merge(ctx[:a_abc], ctx[:b_bcd])) |> Enum.sort
  end
  
  test "delete", ctx do
    dictlist = Keyword.take(ctx, [:a, :c])
    assert dictlist == ORDict.delete(ctx[:a_abc], :b) |> sort_value
    assert ctx[:a_abc] == ORDict.delete(ctx[:a_abc], :d)
    assert {:error, {:precondition, {:not_present, :d}}} == ORDict.remove(ctx[:a_abc], :d)
  end
  
  test "drop", ctx do
    dictlist = Keyword.take(ctx, [:a])
    assert dictlist == ORDict.drop(ctx[:a_abc], [:b,:c]) |> sort_value
  end
  
  test "fetch, fetch!, get", ctx do
    assert {:ok, ctx[:a]} == ORDict.fetch(ctx[:a_abc], :a)
    assert :error == ORDict.fetch(ctx[:a_abc], :d)
    assert ctx[:a] == ORDict.fetch!(ctx[:a_abc], :a)
    
    assert ctx[:a] == ORDict.get(ctx[:a_abc], :a)
    assert nil == ORDict.get(ctx[:a_abc], :d)
    assert ctx[:a] == ORDict.get(ctx[:a_abc], :d, ctx[:a])
  end
  
  test "has_key?", ctx do
    assert ORDict.has_key?(ctx[:a_abc], :a)
    assert !ORDict.has_key?(ctx[:a_abc], :d)
  end
  
  test "keys", ctx do
    assert [:a,:b,:c] == ORDict.keys(ctx[:a_abc]) |> Enum.sort
  end
  
  test "merge", ctx do
    abcdef = Keyword.take(ctx, [:a,:b,:c,:d,:e,:f])
    assert ctx[:a_abc]===ORDict.merge(ctx[:a_abc], ctx[:a_abc])
    assert abcdef == ORDict.merge(ctx[:a_abc], ctx[:b_def]) |> sort_value
  end
  
  test "merge disjoint", ctx do
    [a: a,b: b,b1: b1,c: c,c1: c1,d: d] = Keyword.take(ctx, [:a,:b,:b1,:c,:c1,:d]) |> Enum.sort
    abcd = [a: a,b: GCounter.merge(b,b1),c: GCounter.merge(c,c1),d: d]
    assert abcd == ORDict.merge(ctx[:a_abc], ctx[:b_bcd]) |> sort_value
  end
  
  test "merge disjoint w/ remove", ctx do
    [a: a,b: b,b1: b1,d: d] = Keyword.take(ctx, [:a,:b,:b1,:d]) |> Enum.sort
    abd = [a: a,b: GCounter.merge(b,b1), d: d]
    b_abd = ORDict.merge(ctx[:a_abc], ctx[:b_bcd]) |> ORDict.remove!(:c)
    assert abd == ORDict.merge(ctx[:a_abc], b_abd) |> sort_value
  end
  
  # NOTE: the semantics of this may change soon
  test "merge w/ mismatch crdt", ctx do
    mismatch = ORDict.new() |> ORDict.add!(:b, :c, PNCounter.new)
    # ORDict.merge(mismatch, ctx[:a_abc])
    assert_raise Ravel.CRDTMismatchError, fn -> ORDict.merge(mismatch, ctx[:a_abc]) end
  end
  
  test "pop", ctx do
    # dict_values = Keyword.take(ctx, [:b,:c]) |> Enum.sort
    new_dict = ORDict.remove!(ctx[:a_abc], :a)
    assert {ctx[:a], new_dict} == ORDict.pop(ctx[:a_abc], :a)
  end
  
  test "put", ctx do
    dictlist = [ {:d, ctx[:a]} | Keyword.take(ctx, [:a,:b,:c])] |> Enum.sort
    assert dictlist == ORDict.put(ctx[:a_abc], {:d, :a}, ctx[:a]) |> sort_value
  end
  
  test "put_new", ctx do
    assert_raise KeyError, fn() -> ORDict.put_new(ctx[:a_abc], {:d, :a}, ctx[:a]) end
  end
  
  test "reduce", ctx do
    assert 6 == Enum.reduce(ctx[:a_abc], 0, fn({_,v}, acc) -> GCounter.value(v)+acc end)
    #assert [0,1] == Enum.map(a, &(&1-1)) |> Enum.sort
    assert [{0,{:a, 1}},{1,{:b, 2}},{2,{:c, 3}}] == Stream.zip(0..2, ctx[:a_abc]) |> Enum.map(fn({n,{k,v}}) -> {n,{k,GCounter.value(v)}} end)
  end
  
  test "size", ctx do
    assert 0 == ORDict.size(ctx[:empty])
    assert 3 == ORDict.size(ctx[:a_abc])
  end
  
  test "update", ctx do
    dictlist1 = [{:a,ctx[:a]}, {:b,GCounter.merge(ctx[:b],ctx[:b1])}, {:c,ctx[:c]}]
    dictlist2 = [{:a,ctx[:a]}, {:b, ctx[:b]}, {:c,ctx[:c]}, {:d,ctx[:d]}]
    #IO.inspect ORDict.update(ctx[:a_abc], {:a,:b}, ctx[:b1], &GCounter.merge(&1,ctx[:b1]))
    assert dictlist1 == ORDict.update!(ctx[:a_abc], {:a,:b}, &GCounter.merge(&1,ctx[:b1])) |> sort_value
    assert dictlist2 == ORDict.update(ctx[:a_abc], {:a,:d}, ctx[:d],  &GCounter.merge(&1,ctx[:d]))  |> sort_value
    assert dictlist1 == ORDict.update(ctx[:a_abc], {:a,:b}, ctx[:b1], &GCounter.merge(&1,ctx[:b1])) |> sort_value
    assert_raise KeyError, fn -> ORDict.update!(ctx[:a_abc], {:a,:d}, &GCounter.merge(&1,ctx[:b1])) end
  end
  
  test "values", ctx do
    assert [ctx[:a],ctx[:b],ctx[:c]] == ORDict.values(ctx[:a_abc]) |> Enum.sort
  end
  
  defp sort_value(ordict) do
    ordict |> ORDict.value |> Enum.sort
  end
  
end
