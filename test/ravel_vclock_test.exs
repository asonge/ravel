defmodule RavelVClockTest do
  use ExUnit.Case, async: true
  alias Ravel.VClock
  
  # setup do
  #     { :ok, [a: Ravel.GCounter.new()] }
  # end
  
  test "simple" do
    a = VClock.new()
    b = VClock.new()
    a1 = VClock.increment(a, :actor1)
    b1 = VClock.increment(b, :actor2)
    assert true == VClock.descends(a1, a)
    assert true == VClock.descends(b1, b)
    assert false == VClock.descends(a1, b1)
    a2 = VClock.increment(a1, :actor1)
    c = VClock.merge([a2, b1])
    c1 = VClock.increment(c, :actor3)
    assert true == VClock.descends(c1, a2)
    assert true == VClock.descends(c1, b1)
    assert false == VClock.descends(b1, c1)
    assert false == VClock.descends(b1, a1)
  end
  
  test "accessor" do
    vc = VClock.new() |> VClock.increment(:actor1) |> VClock.increment(:actor2) |> VClock.increment(:actor2)
    assert 1 == VClock.get_counter(vc, :actor1)
    assert 2 == VClock.get_counter(vc, :actor2)
    assert 0 == VClock.get_counter(vc, :actor3)
    assert [:actor1, :actor2] == VClock.nodes(vc)
  end
  
  test "merge_test" do
    vc1 = VClock.new([actor1: 1, actor2: 2, actor4: 4])
    vc2 = VClock.new([actor3: 3, actor4: 3])
    vc3 = VClock.new([actor1: 1, actor2: 2, actor3: 3, actor4: 4])
    assert vc3 == VClock.merge([vc1, vc2])
  end
 
  test "merge_less_left_right_test" do
    vc1 = VClock.new([actor5: 5])
    vc2 = VClock.new([actor6: 6, actor7: 7])
    vc3 = VClock.new([actor5: 5, actor6: 6, actor7: 7])
    assert vc3 == VClock.merge([vc1, vc2])
    assert vc3 == VClock.merge([vc2, vc1])
  end

  test "merge_same_id_test" do
    vc1 = VClock.new([actor1: 1, actor2: 1])
    vc2 = VClock.new([actor1: 1, actor3: 1])
    vc3 = VClock.new([actor1: 1, actor2: 1, actor3: 1])
    assert vc3 == VClock.merge([vc1, vc2])
    assert vc3 == VClock.merge([vc2, vc1])
  end
  
  test "dominates" do
    a = VClock.new([a: 1, b: 2])
    b = VClock.new([b: 2])
    c = VClock.new([c: 2])
    d = VClock.new([b: 1])
    e = VClock.new([a: 1])
    assert VClock.dominates(a,b)
    assert false == VClock.dominates(a,a)
    assert VClock.dominates(a,e)
    assert false == VClock.dominates(a,c)
    assert VClock.dominates(a,d)
  end
  
  test "equal" do
    assert VClock.equal(VClock.new([a: 1, b: 2]), VClock.new([b: 2, a: 1]))
    assert false == VClock.equal(VClock.new([a: 1, b: 2]), VClock.new([b: 2, a: 1, c: 2]))
  end
  
  test "replace_actors" do
    a = VClock.new([a: 1, b: 2, c: 3, d: 4])
    b = VClock.new([e: 1, f: 2, c: 3, d: 4])
    assert VClock.equal(b, VClock.replace_actors(a, [e: :a, f: :b]))
    assert VClock.equal(b, VClock.replace_actors(a, [e: :a, f: :b, h: :g]))
  end
  
end
