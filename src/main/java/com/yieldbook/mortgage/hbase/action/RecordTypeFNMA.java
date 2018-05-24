package com.yieldbook.mortgage.hbase.action;

import java.util.HashMap;
import java.util.Map;

public enum RecordTypeFNMA {
	POOL_LEVEL(1),
	LOAN_LEVEL(2);

	private final int value;

    RecordTypeFNMA(final int newValue) {
        value = newValue;
    }

    public int getValue() { return value; }	
    
    // Mapping int to enum
    private static final Map<Integer, RecordTypeFNMA> _map = new HashMap<Integer, RecordTypeFNMA>();
    static
    {
        for (RecordTypeFNMA loanType : RecordTypeFNMA.values())
            _map.put(loanType.value, loanType);
    }

    /**
    * Get RecordType from value
    * @param value Value
    * @return LOANTYPE
    */
    public static RecordTypeFNMA from(int value)
    {
        return _map.get(value);
    }   
    
}
