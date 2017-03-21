package net.devneko.kjdbc

import java.sql.Connection

open class SqlHelper
(
        val connection:Connection
)
{
    fun queryOriginal(sql:String, block: (ParameterMapper.()->Unit)? = null): ResultSetWrapper {
        val analyzeResult = SqlAnalyzer.analyze(sql)
        val ps = connection.prepareStatement(analyzeResult.sql)
        val mapper = ParameterMapper(analyzeResult.nameIndex, ps)
        block?.let {
            mapper.it()
        }
        return ResultSetWrapper(ps, ps.executeQuery())
    }

    fun selectionColumns(selection:String, block:(ColumnNameMapper.()->Unit)? = null):String {
        if (selection.isEmpty()) {return "*"}
        val mapper = ColumnNameMapper(selection)
        block?.let {
            mapper.it()
        }
        return mapper.selectColumns
    }

    fun query(tableName:String, block:UpdateParameterAndConditionBulder.()->Unit): ResultSetWrapper {
        val builder = UpdateParameterAndConditionBulder()
        builder.block()
        val selectColumns = selectionColumns(builder.selection, builder.selectionBlock())
        val whereClause = if ( builder.condition.isNotEmpty() ) "WHERE ${builder.condition}" else ""
        val order = if ( builder.orderBy.isNotEmpty() ) "ORDER BY ${builder.orderBy}" else ""
        val limit = if ( builder.limit.isNotEmpty() ) "LIMIT ${builder.limit}" else ""
        val sql = "SELECT ${selectColumns} FROM `$tableName` $whereClause $order $limit"
        return queryOriginal(sql, builder.parameterMapper())
    }

    fun count(tableName:String, condition:String = "", block:(ParameterMapper.()->Unit)? = null):Int {
        val whereClause = if ( condition.isNotEmpty() ) "WHERE ${condition}" else ""
        val sql = "SELECT COUNT(*) as `cnt` FROM `$tableName` $whereClause"
        val rs = queryOriginal(sql, block)
        try {
            rs.next()
            return rs.get("cnt")
        } finally {
            rs.close()
        }
    }

    fun insert(tableName:String, block:UpdateParameterBuilder.()->Unit):Int {
        val builder = UpdateParameterBuilder()
        builder.block()
        val nameList = builder.nameList
        var setColumns = nameList.map{ "`$it`" }.joinToString(",")
        var setValues =  nameList.map{ ":$it" }.joinToString(", ")
        for((k, v) in builder.setAsList) {
            setColumns += ",`${k}`"
            setValues += ", ${v}"
        }
        val sql = "INSERT INTO `$tableName` ($setColumns) VALUES ($setValues)"
        return updateSql(sql, builder.parameterMapper())
    }

    fun update(tableName:String, block:UpdateParameterAndConditionBulder.()->Unit):Int {
        val builder = UpdateParameterAndConditionBulder()
        builder.block()
        val nameList = builder.nameList
        var setItems = nameList.map{ "`$it` = :$it" }.joinToString(", ")
        for((k, v) in builder.setAsList) {
            setItems += ", `${k}` = ${v}"
        }
        val whereClause = if ( builder.condition.isNotEmpty() ) "WHERE ${builder.condition}" else ""
        val sql = "UPDATE `$tableName` SET $setItems $whereClause"
        return updateSql(sql, builder.parameterMapper())
    }

    fun deleteOriginal(tableName:String, condition:String, block: ParameterMapper.()->Unit):Int {
        val sql = "DELETE FROM `$tableName` WHERE $condition"
        return updateSql(sql, block)
    }

    fun delete(tableName:String, block:UpdateParameterAndConditionBulder.()->Unit):Int {
        val builder = UpdateParameterAndConditionBulder()
        builder.block()
        val whereClause = if ( builder.condition.isNotEmpty() ) "WHERE ${builder.condition}" else ""
        val sql = "DELETE FROM `$tableName` $whereClause"
        return updateSql(sql, builder.parameterMapper())
    }

    fun prepare(sql:String):SmartPreparedStatement {
        val analyzeResult = SqlAnalyzer.analyze(sql)
        val ps = connection.prepareStatement(analyzeResult.sql)
        val mapper = ParameterMapper(analyzeResult.nameIndex, ps)
        return SmartPreparedStatement(mapper)
    }

    fun updateSql(sql:String, block: ParameterMapper.()->Unit):Int {
        val ps = prepare(sql)
        try {
            return ps.executeUpdate(block)
        } finally {
            ps.close()
        }
    }

    /**
     * generate placeholder string for "in clause".
     *
     * for example.
     * placeholdersForIn(arrayOf("a", "b"), "id) return "(:id1, :id2, :id3
     */
    fun placeholdersForIn(values:List<Any>, prefix:String):String {
        if ( values.size == 0 ) {
            throw IllegalArgumentException("values size must be greater than zero.")
        }
        return "(" + (0..(values.size-1)).map { ":${prefix}${it}" }.joinToString(",") + ")"
    }

    fun lastInsertId():Int {
        return queryOriginal("SELECT LAST_INSERT_ID()").let {
            val result = it.map { getInt(1) }.first()
            it.close()
            result
        }
    }
}
