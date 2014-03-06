# would be nice to use refinements here, but that breaks 2.0 compatibility
class Method
  def kwargs_as_hash( invocation_binding )
    named_locals = parameters. \
      select{|type,_| type == :key}. \
      flat_map{|_,name| [name,invocation_binding.eval(name.to_s)]}

    Hash[ *named_locals ]
  end
end
