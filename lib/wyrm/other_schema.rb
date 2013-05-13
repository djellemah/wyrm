# Place for stuff that I'm not sure about yet
class OtherSchema
  def same_db
    @dst_db.andand.database_type == @src_db.andand.database_type
  end
end
