package com.recon.reconcile

import scala.collection.mutable.ArrayBuffer

class ruleDataViewRecord {
  
  
    private var _priority: Int = _
    def priority = _priority
    def priority_= (priorityValue :Int) :Unit ={
      _priority = priorityValue
    }
    
    private var _ruleId: Long = _
    def ruleId = _ruleId
    def ruleId_= (ruleIdValue :Long) :Unit ={
      _ruleId = ruleIdValue
    }

    private var _ruleName: String = _
    def ruleName = _ruleName
    def ruleName_= (ruleNameValue :String) :Unit ={
      _ruleName = ruleNameValue
    }

    private var _ruleType: String = _
    def ruleType = _ruleType
    def ruleType_= (ruleTypeValue :String) :Unit ={
      _ruleType = ruleTypeValue
    }

    private var _sourceViewId: Long = _
    def sourceViewId = _sourceViewId
    def sourceViewId_= (sourceViewIdValue :Long) :Unit ={
      _sourceViewId = sourceViewIdValue
    }

    private var _sourceViewName: String = _
    def sourceViewName = _sourceViewName
    def sourceViewName_= (sourceViewNameValue :String) :Unit ={
      _sourceViewName = sourceViewNameValue
    }    

    private var _sourceFileTemplate: Long = _
    def sourceFileTemplate = _sourceFileTemplate
    def sourceFileTemplate_= (sourceFileTemplateValue :Long) :Unit ={
      _sourceFileTemplate = sourceFileTemplateValue
    }    

    private var _targetViewId: Long = _
    def targetViewId = _targetViewId
    def targetViewId_= (targetViewIdValue :Long) :Unit ={
      _targetViewId = targetViewIdValue
    }    

    private var _targetFileTemplate: Long = _
    def targetFileTemplate = _targetFileTemplate
    def targetFileTemplate_= (targetFileTemplateValue :Long) :Unit ={
      _targetFileTemplate = targetFileTemplateValue
    }    

    private var _targetViewName: String = _
    def targetViewName = _targetViewName
    def targetViewName_= (targetViewNameValue :String) :Unit ={
      _targetViewName = targetViewNameValue
    }        

    private var _ruleConditions: ArrayBuffer[ruleConditionRecord] = _
    def ruleConditions = _ruleConditions
    def ruleConditions_= (ruleConditionsValue :ArrayBuffer[ruleConditionRecord]) :Unit ={
      _ruleConditions = ruleConditionsValue
    }    
    
    override def toString(): String = {
     "@Rule Record [priority=" + _priority + ", ruleId=" + _ruleId +
				 ", ruleName=" + _ruleName + ", ruleType=" + _ruleType +
				 ", sourceViewId=" + _sourceViewId + ", sourceViewName=" +_sourceViewName + 
				 ", sourceFileTemplate=" + _sourceFileTemplate 	+ ", targetViewId=" + _targetViewId + 
				 ", targetViewName="	+ _targetViewName + ", targetFileTemplate=" + _targetFileTemplate +
				 ", ruleConditions=" + _ruleConditions + "]"
    }
    
}