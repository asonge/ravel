
defmodule Ravel.LWWRegister do
  
  defrecordp :s, __MODULE__, timestamp: nil, value: nil
  
  def new() do
    s(timestamp: nil, value: nil)
  end
  
  def merge(lww1, lww2) when lww1 == lww2, do: lww1
  def merge(s(timestamp: t1)=lww1, s(timestamp: t2)) when t1 > t2, do: lww1
  def merge(s(timestamp: t1), s(timestamp: t2)=lww2) when t1 < t2, do: lww2
  def merge(s(value: v1)=lww1, s(value: v2)) when v1 > v2, do: lww1
  def merge(_, lww2), do: lww2
  
  def set(register, value) do
    s(register, timestamp: get_timestamp(), value: value)
  end
  
  def value(s(value: value)) do
    value
  end
  
  defp get_timestamp() do
    # For now we have to go to the land that erlang built to get time.
    :os.timestamp()
  end
  
end

defimpl Ravel.CRDT, for: Ravel.LWWRegister do
  alias Ravel.LWWRegister
  
  def actors(_),           do: []
  def equal?(crdt1, crdt2),       do: crdt1 === crdt2
  def equal_value?(crdt1, crdt2), do: CRDT.value(crdt1) === CRDT.value(crdt2)
  def merge(_, crdt2) when elem(crdt2,0)!==LWWRegister do
    raise Ravel.CRDTMismatchError [type1: LWWRegister, type2: elem(crdt2, 0)]
  end
  def merge(crdt1, crdt2), do: LWWRegister.merge(crdt1, crdt2)
  def value(crdt),         do: LWWRegister.value(crdt)
  
end

defimpl Inspect, for: Ravel.LWWRegister do
  import Inspect.Algebra
  
  def inspect({Ravel.LWWRegister, timestamp, value}, opts) do
    case opts.pretty do
      false ->
        concat ["#LWWRegister<#{timestamp}, #{value}>"]
      true ->
        "#LWWRegister<#{value}>"
    end
  end
end
