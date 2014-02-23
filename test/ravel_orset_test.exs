
defmodule RavelORSetTest do
  use ExUnit.Case, async: true
  use ExCheck
  alias Ravel.CRDT
  alias Ravel.ORSet
  
  setup do
    {:ok, [
      empty: ORSet.new,
      a_123: ORSet.new |> ORSet.add!(:a, 1) |> ORSet.add!(:a, 2) |> ORSet.add!(:a, 3),
      a_123_m: ORSet.new(Enum.map 1..3, &{:a, &1}),
      b_234: ORSet.new |> ORSet.add!(:b, 2) |> ORSet.add!(:b, 3) |> ORSet.add!(:b, 4),
      b_456: ORSet.new(4..6, :b)
    ]}
  end
  
  test "simple check", context do
    assert [] == ORSet.to_list(context[:empty])
    assert [1,2,3] == ORSet.to_list(context[:a_123]) |> Enum.sort
    assert [1,2,3] == ORSet.to_list(context[:a_123_m]) |> Enum.sort
    assert {:error, :need_actor_tuple} == ORSet.new([1,2,3])
    assert [1,2,3] == ORSet.to_list(ORSet.new(1..3, :a)) |> Enum.sort
  end
  
  test "actors", ctx do
    assert [:a] === CRDT.actors(ctx[:a_123])
    assert [:a,:b] === CRDT.actors(CRDT.merge(ctx[:a_123], ctx[:b_234])) |> Enum.sort
  end
  
  test "merge", context do
    assert [1,2,3,4,5,6] == ORSet.merge(context[:a_123], context[:b_456]) |> sort_value
    assert [1,2,3,4,5,6] == ORSet.union(context[:a_123], context[:b_456]) |> sort_value
  end
    
  test "complex merge", context do
    a = context[:a_123]
    b = context[:b_234]
    c1 = ORSet.merge(a, b)
    c2 = ORSet.merge(b, a)
    assert [1,2,3,4] == ORSet.value(c1) |> Enum.sort
    assert [1,2,3,4] == ORSet.value(c2) |> Enum.sort
    a1 = ORSet.add!(a, :a, 5)
    assert [1,2,3,4,5] == ORSet.merge(a1,c1) |> sort_value
    a2 = ORSet.remove!(a1, 1)
    assert [2,3,4,5] == ORSet.merge(a2,c1) |> sort_value
  end
  
  test "member?, delete", context do
    a = context[:a_123]
    
    assert true == ORSet.member?(a, 2)
    assert false == ORSet.member?(a, 4)
    
    b = context[:b_456]
    c = ORSet.merge(a,b)
    
    assert true == ORSet.member?(c, 2)
    assert true == ORSet.member?(c, 4)
    
    c = ORSet.delete(c, 2)
    assert false == ORSet.member?(c, 2)
  end
  
  test "size", context do
    a = context[:a_123]
    assert 0 == ORSet.size(ORSet.new())
    assert 3 == ORSet.size(a)
    a = ORSet.remove!(a, 1)
    assert 2 == ORSet.size(a)
  end
  
  test "double add" do
    a = ORSet.new
    a = ORSet.add!(a, :a, 1)
    a = ORSet.add!(a, :b, 1)
    assert [1] == ORSet.value(a) |> Enum.to_list
  end
  
  test "double add merge" do
    a = ORSet.new
    b = ORSet.add!(a, :a, 1)
    c = ORSet.add!(a, :b, 1)
    d = ORSet.merge(b, c)
    assert [1] == ORSet.value(d) |> Enum.to_list
  end
  
  test "disjoint merge drop" do
    a = ORSet.new([a: 1, a: -2])
    b = ORSet.new([b: 4])
    c = ORSet.merge(a, b)
    a = ORSet.remove!(a, -2)
    assert false == ORSet.merge(a, c) |> ORSet.member?(-2)
    c = ORSet.add!(c, :c, -2)
    assert true == ORSet.merge(a, c) |> ORSet.member?(-2)
  end
  
  test "disjoint?" do
    a = ORSet.new([a: 1, a: -2])
    b = ORSet.new([b: 4])
    assert ORSet.disjoint?(a, b)
    c = ORSet.merge(a, b)
    a = ORSet.remove!(a, -2)
    assert false == ORSet.disjoint?(a, c)
    c = ORSet.add!(c, :c, -2) |> ORSet.remove!(1)
    assert false == ORSet.disjoint?(a, c)
    a = ORSet.add!(a, :a, 1)
    assert ORSet.disjoint?(a, c)
  end
  
  test "empty", context do
    assert ORSet.new() === ORSet.empty(context[:a_123])
  end
  
  test "equal?", context do
    c = ORSet.merge(context[:a_123], context[:b_234])
    assert CRDT.equal_value?(ORSet.new(1..4, :c), c)
    assert CRDT.equal?(context[:a_123], context[:a_123])
  end
  
  test "difference" do
    a = ORSet.new |> ORSet.add!(:a, 1) |> ORSet.add!(:a, 2)
    b = ORSet.new |> ORSet.add!(:b, 2)
    assert [1] == ORSet.difference(a,b) |> ORSet.to_list
  end
  
  test "intersection", context do
    assert [2,3] === ORSet.intersection(context[:a_123], context[:b_234]) |> sort_value
    assert [] === ORSet.intersection(context[:a_123], context[:b_456]) |> sort_value
  end
  
  test "put" do
    assert [1] === ORSet.new() |> ORSet.put({:a, 1}) |> ORSet.to_list
  end
  
  test "subset" do
    assert ORSet.subset?(ORSet.new(1..5, :a), ORSet.new(1..8, :b))
    assert false === ORSet.subset?(ORSet.new(1..5, :a), ORSet.new(2..8, :b))
  end
  
  test "disjoint drop using difference instead of merge" do
    a = ORSet.new |> ORSet.add!(:a, 1) |> ORSet.add!(:a, -2)
    b = ORSet.new |> ORSet.add!(:b, 4)
    c = ORSet.merge(a, b)
    a = ORSet.remove!(a, -2)# |> ORSet.add!(:a, 5)
    assert false == ORSet.difference(a, c) |> ORSet.member?(-2)
    c = ORSet.add!(c, :c, -2)
    assert false == ORSet.difference(a, c) |> ORSet.member?(-2)
  end
  
  test "Enumerable count/member? support" do
    a = ORSet.new |> ORSet.add!(:a, 1) |> ORSet.add!(:a, 2)
    assert Enum.count(a) == 2
    assert Enum.member?(a, 1)
  end
  
  test "Enumerable reduce support" do
    a = ORSet.new |> ORSet.add!(:a, 1) |> ORSet.add!(:a, 2)
    assert 3 == Enum.reduce(a, 0, &(&1+&2))
    assert [0,1] == Enum.map(a, &(&1-1)) |> Enum.sort
    assert Enum.zip(0..1, CRDT.value(a)) == Stream.zip(0..1, a) |> Enum.to_list
  end
  
  property :adds do
    for_all {x, y} in {list(int), list(int)} do
      values = Enum.sort(x ++ y) |> Enum.uniq
      a = Enum.reduce(x, ORSet.new, fn(v, acc) -> ORSet.add!(acc, :a, v) end)
      b = Enum.reduce(y, ORSet.new, fn(v, acc) -> ORSet.add!(acc, :b, v) end)
      values == ORSet.merge(a, b) |> sort_value
    end
  end
  
  property :add_subtract_add do
    for_all {adds, subtracts, readds} in {list(int), list(int), list(int)} do
      adds = Enum.uniq adds
      subtracts = Enum.uniq subtracts
      readds = Enum.uniq readds
      values = Enum.sort((adds -- subtracts) ++ readds) |> Enum.uniq
      
      a = Enum.reduce(adds, ORSet.new, fn(v, acc) -> ORSet.add!(acc, :a, v) end)
      b = Enum.reduce(subtracts, a, fn(v, acc) ->
        case ORSet.member?(a, v) do
          true -> ORSet.remove!(acc, v)
          false -> acc
        end
      end)
      c = Enum.reduce(readds, ORSet.new, fn(v, acc) -> ORSet.add!(acc, :b, v) end)
      
      values == ORSet.merge(b, c) |> sort_value
    end
  end
  
  defp sort_value(orset) do
    CRDT.value(orset) |> Enum.sort
  end
  
end
