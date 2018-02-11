package com.recon.reconcile

class ruleConditionRecord() {
    
    private var _id: Long = _               // private variable
    def id = _id                            // getter method
    def id_= (idValue:Long):Unit = {        // setter method 
      _id = idValue 
    }
    
    private var _openBracket: String = _
    def openBracket = _openBracket
    def openBracket_= (openBracketValue :String) :Unit ={
      _openBracket = openBracketValue
    }
      
    private var _sColumnId : Long = _
    def sColumnId = _sColumnId
    def sColumnId_= (sColumnIdValue: Long) : Unit = {
      _sColumnId = sColumnIdValue
    }
    
    private var _sRefDvColumn: String = _
    def sRefDvColumn = _sRefDvColumn;
    def sRefDvColumn_= (sRefDvColumnValue:String):Unit = {
      _sRefDvColumn = sRefDvColumnValue 
    }
    
    private var _sColumnName: String = _
    def sColumnName = _sColumnName
    def sColumnName_=(sColumnNameValue : String) = {
      _sColumnName = sColumnNameValue
    }
      
      
    private var _sColDataType: String = _
    def sColDataType = _sColDataType
    def sColDataType_= (sColDataTypeValue :String ):Unit = {
      _sColDataType = sColDataTypeValue
    }
    
    private var _sColDataFormat: String = _
    def sColDataFormat = _sColDataFormat
    def sColDataFormat_= (sColDataFormatValue : String) :Unit = {
      _sColDataFormat = sColDataFormatValue
    }
    
    private var _sFormula: String = _
    def sFormula = _sFormula
    def sFormula_= (sFormulaValue: String):Unit = {
      _sFormula = sFormulaValue
    }
    
    private var _sToleranceType: String = _
    def sToleranceType = _sToleranceType
    def sToleranceType_=(sToleranceTypeValue: String) :Unit = {
      _sToleranceType = sToleranceTypeValue
    }
    private var _sMany: Boolean = _
    def sMany = _sMany
    def sMany_=(sManyValue : Boolean) : Unit = {
      _sMany = sManyValue
    }
    
    private var _tColumnId: Long = _
    def tColumnId = _tColumnId
    def tColumnId_= (tColumnIdValue: Long) : Unit = {
      _tColumnId = tColumnIdValue
    }
    
    private var _tRefDvColumn: String =_
    def tRefDvColumn = _tRefDvColumn
    def tRefDvColumn_= (tRefDvColumnValue: String) : Unit = {
      _tRefDvColumn= tRefDvColumnValue
    }
    private var _tColumnName: String = _
    def tColumnName = _tColumnName
    def tColumnName_= (tColumnNameValue : String) : Unit = {
      _tColumnName = tColumnNameValue
    }
    
    private var _tColDataType: String =_
    def tColDataType = _tColDataType
    def tColDataType_= (tColDataTypeValue : String) :Unit = {
      _tColDataType = tColDataTypeValue
    }
    
    private var _tColDataFormat: String = _
    def  tColDataFormat = _tColDataFormat
    def tColDataFormat_= ( tColDataFormatValue : String):Unit = {
      _tColDataFormat = tColDataFormatValue
    }    

    private var _tFormula: String = _
    def tFormula = _tFormula
    def tFormula_= ( tFormulaValue : String):Unit = {
      _tFormula = tFormulaValue
    }

    private var _tMany: Boolean = _
    def tMany = _tMany
    def tMany_= ( tManyValue : Boolean):Unit = {
      _tMany = tManyValue
    }

    private var _tToleranceType: String = _
    def tToleranceType = _tToleranceType
    def tToleranceType_= ( tToleranceTypeValue : String):Unit = {
      _tToleranceType = tToleranceTypeValue
    }

    private var _closeBracket: String = _
    def closeBracket = _closeBracket
    def closeBracket_= ( closeBracketValue : String):Unit = {
      _closeBracket = closeBracketValue
    }
    
    private var _logicalOperator: String = _
    def logicalOperator = _logicalOperator
    def logicalOperator_= ( logicalOperatorValue : String):Unit = {
      _logicalOperator = logicalOperatorValue
    }

    private var _createdBy: Long = _
    def createdBy = _createdBy
    def createdBy_= (createdByValue : Long):Unit = {
      _createdBy = createdByValue
    }

    private var _lastUpdatedBy: Long = _
    def lastUpdatedBy = _lastUpdatedBy
    def lastUpdatedBy_= (lastUpdatedByValue : Long):Unit = {
      _lastUpdatedBy = lastUpdatedByValue
    }
    
    private var _sColumnFieldName: String = _
    def sColumnFieldName = _sColumnFieldName
    def sColumnFieldName_= ( sColumnFieldNameValue : String):Unit = {
      _sColumnFieldName = sColumnFieldNameValue
    }

    private var _sToleranceOperatorFrom: String = _
    def sToleranceOperatorFrom = _sToleranceOperatorFrom
    def sToleranceOperatorFrom_= ( sToleranceOperatorFromValue : String):Unit = {
      _sToleranceOperatorFrom = sToleranceOperatorFromValue
    }

    private var _sToleranceValueFrom: String = _
    def sToleranceValueFrom = _sToleranceValueFrom
    def sToleranceValueFrom_= ( sToleranceValueFromValue : String):Unit = {
      _sToleranceValueFrom = sToleranceValueFromValue
    }    
value
    private var _sToleranceOperatorTo: String = _
    def sToleranceOperatorTo = _sToleranceOperatorTo
    def sToleranceOperatorTo_= ( sToleranceOperatorToValue : String):Unit = {
      _sToleranceOperatorTo = sToleranceOperatorToValue
    }        

    private var _sToleranceValueTo: String = _
    def sToleranceValueTo = _sToleranceValueTo
    def sToleranceValueTo_= ( sToleranceValueToValue : String):Unit = {
      _sToleranceValueTo = sToleranceValueToValue
    }    

    private var _tToleranceOperatorFrom: String = _
    def tToleranceOperatorFrom = _tToleranceOperatorFrom
    def tToleranceOperatorFrom_= ( tToleranceOperatorFromValue : String):Unit = {
      _tToleranceOperatorFrom = tToleranceOperatorFromValue
    }    
    
    private var _tToleranceValueFrom: String = _
    def tToleranceValueFrom = _tToleranceValueFrom
    def tToleranceValueFrom_= ( tToleranceValueFromValue : String):Unit = {
      _tToleranceValueFrom = tToleranceValueFromValue
    }    

    private var _tToleranceOperatorTo: String = _
    def tToleranceOperatorTo = _tToleranceOperatorTo
    def tToleranceOperatorTo_= ( tToleranceOperatorToValue : String):Unit = {
      _tToleranceOperatorTo = tToleranceOperatorToValue
    }        

    private var _tToleranceValueTo: String = _
    def tToleranceValueTo = _tToleranceValueTo
    def tToleranceValueTo_= ( tToleranceValueToValue : String):Unit = {
      _tToleranceValueTo = tToleranceValueToValue
    }            

    private var _sValueType: String = _
    def sValueType = _sValueType
    def sValueType_= ( sValueTypeValue : String):Unit = {
      _sValueType = sValueTypeValue
    }            

    private var _sOperator: String = _
    def sOperator = _sOperator
    def sOperator_= ( sOperatorValue : String):Unit = {
      _sOperator = sOperatorValue
    }            
    
    private var _sValue: String = _
    def sValue = _sValue
    def sValue_= ( sValueValue : String):Unit = {
      _sValue = sValueValue
    }            

    private var _tValueType: String = _
    def tValueType = _tValueType
    def tValueType_= ( tValueTypeValue : String):Unit = {
      _tValueType = tValueTypeValue
    }            
    
    private var _tOperator: String = _
    def tOperator = _tOperator
    def tOperator_= ( tOperatorValue : String):Unit = {
      _tOperator = tOperatorValue
    }            

    private var _tValue: String = _
    def tValue = _tValue
    def tValue_= (tValueValue : String):Unit = {
      _tValue = tValueValue
    }            

    private var _valueType: String = _
    def valueType = _valueType
    def valueType_= ( valueTypeValue : String):Unit = {
      _valueType = valueTypeValue 
    }                

    private var _operator: String = _
    def operator = _operator
    def operator_= ( operatorValue : String):Unit = {
      _operator = operatorValue
    }          

    private var _value: String = _
    def value = _value
    def value_= ( valueValue : String):Unit = {
      _value = valueValue
    }                    

    override def toString():String = {
      
      "@ ruleDataViewRecord [id=" + _id + ", openBracket=" + _openBracket +
				 ", sColumnId=" + _sColumnId + ", sRefDvColumn=" + _sRefDvColumn +
				 ", sColumnName=" + _sColumnName + ", sColDataType=" + _sColDataType + 
				 ", sColDataFormat=" + _sColDataFormat + ", sFormula=" + _sFormula + 
				 ", sToleranceType=" + _sToleranceType + ", sMany=" + _sMany + 
				 ", tColumnId="	+ _tColumnId + ", tRefDvColumn=" + _tRefDvColumn +
				 ", tColumnName=" + _tColumnName + ", tColDataType=" + _tColDataType + 
				 ", tColDataFormat=" + _tColDataFormat + ", tFormula=" + _tFormula + 
				 ", tMany=" + _tMany	+ ", tToleranceType=" + _tToleranceType + 
				 ", closeBracket="	+ _closeBracket + ", logicalOperator=" + _logicalOperator + 
				 ", createdBy=" + _createdBy + ", lastUpdatedBy=" + _lastUpdatedBy + 
				 ", sColumnFieldName=" + _sColumnFieldName + ", sToleranceOperatorFrom=" + _sToleranceOperatorFrom +
				 ", sToleranceValueFrom=" + _sToleranceValueFrom	+ ", sToleranceOperatorTo=" + _sToleranceOperatorTo +
				 ", sToleranceValueTo=" + _sToleranceValueTo 	+ ", tToleranceOperatorFrom=" + _tToleranceOperatorFrom +
				 ", tToleranceValueFrom=" + _tToleranceValueFrom + ", tToleranceOperatorTo=" + _tToleranceOperatorTo +
				 ", tToleranceValueTo=" + _tToleranceValueTo + ", sValueType=" + _sValueType + 
				 ", sOperator=" + _sOperator + ", sValue=" + _sValue + ", tValueType=" + _tValueType + 
				 ", tOperator="	+ _tOperator + ", tValue=" + _tValue + ", valueType=" + _valueType +
				 ", operator=" + _operator + ", value=" + _value + "]"
      
    }
}