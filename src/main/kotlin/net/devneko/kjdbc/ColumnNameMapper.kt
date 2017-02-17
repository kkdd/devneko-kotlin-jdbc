package net.devneko.kjdbc

class ColumnNameMapper
(
    var selectColumns: String
)
{
    fun get():String {
        return selectColumns
    }

    fun AS(name:String, value:String) {
      val columnNameRegex = Regex(":" + name)
      val columnNameReplace = value + " AS " + name
      selectColumns = columnNameRegex.replace(selectColumns, columnNameReplace)
    }
}
