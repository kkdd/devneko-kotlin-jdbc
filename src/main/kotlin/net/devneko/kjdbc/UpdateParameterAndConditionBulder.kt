package net.devneko.kjdbc

class UpdateParameterAndConditionBulder() : UpdateParameterBuilder() {

    private var _cond:String = ""
    public val condition:String get() = _cond

    private var _select:String = ""
    public val selection:String get() = _select

    private var _limit:String = ""
    public val limit:String get() = _limit

    private var _orderBy:String = ""
    public val orderBy:String get() = _orderBy

    protected val _columnList = arrayListOf<(ColumnNameMapper)->Unit>()

    fun selectionBlock(): ColumnNameMapper.()->Unit {
        return {
           _columnList.forEach { it(this) }
        }
    }

    fun select(select:String, block: (ColumnNameMapper.()->Unit)? = null):Unit {
        this._select = select
        block?.let {
            _columnList.add(block)
        }
    }

    fun where(cond:String, block: (ParameterMapper.()->Unit)? = null ):Unit {
        this._cond = cond
        block?.let {
            _setList.add(block)
        }
    }

    fun limit(limit:Int):Unit {
        this._limit = limit.toString()
    }

    fun orderBy(name:String):Unit {
        this._orderBy = name
    }
}
