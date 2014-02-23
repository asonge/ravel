
defprotocol Ravel.CRDT do
  
  @type actor :: any
  @type t :: any
  
  @doc "List the actors"
  @spec actors(t) :: [actor]
  def actors(crdt)
  
  @doc "See if CRDT's are equal, not value-equal"
  @spec equal?(t, t) :: boolean
  def equal?(crdt1, crdt2)
  
  @doc "See if CRDT's values are equal"
  @spec equal_value?(t, t) :: boolean
  def equal_value?(crdt1, crdt2)
  
  @doc "Merge the crdts"
  @spec merge(t, t) :: t
  def merge(crdt1, crdt2)
  
  @doc "Get the primitive value of the crdt"
  @spec value(t) :: term
  def value(crdt)
  
end
