
defexception Ravel.CRDTMismatchError, [type1: nil, type2: nil] do
  def message(exception) do
    "Mismatch between CRDT's #{inspect exception.type1} !== #{inspect exception.type2}"
  end
end
