
defmodule RavelLWWRegisterTest do
  use ExUnit.Case , async: true
  
  alias Ravel.CRDT
  alias Ravel.LWWRegister
  
  setup do
    { :ok, [
      one: LWWRegister.new |> LWWRegister.set("first post"),
      two: LWWRegister.new |> LWWRegister.set("second post"),
      three: LWWRegister.new |> LWWRegister.set("third post"),
      ts1: {Ravel.LWWRegister, 1, "hack"},
      ts2: {Ravel.LWWRegister, 1, "hacktwo"}
    ] }
  end
  
  test "basic", ctx do
    assert "first post" == ctx[:one] |> CRDT.value
    assert ctx[:one] == CRDT.merge(ctx[:one], ctx[:one])
    assert "third post" == CRDT.merge(ctx[:one], ctx[:three]) |> CRDT.value
    assert "second post" == CRDT.merge(ctx[:two],ctx[:one]) |> CRDT.value
  end
  
  test "same timestamp", ctx do
    assert "hacktwo" == CRDT.merge(ctx[:ts2], ctx[:ts1]) |> CRDT.value
    assert "hacktwo" == CRDT.merge(ctx[:ts1], ctx[:ts2]) |> CRDT.value
  end
  
end
