package net.devneko.kjdbc

import java.sql.*

class ColumnNameMapper
(
    var _selectColumns: String
)
{
    public val selectColumns:String
        get() {return _selectColumns}

    fun AS(name:String, value:String) {
      val columnNameRegex = Regex(":" + name)
      val columnNameReplace = value + " AS " + name
      _selectColumns = columnNameRegex.replace(selectColumns, columnNameReplace)
    }
}
