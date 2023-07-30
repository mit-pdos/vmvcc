package main

/**
 * Based on:
 * https://github.com/apavlo/py-tpcc/blob/7c3ff501bbe98a6a7abd3c13267523c3684b62d6/pytpcc/constants.py
 */

/* Item */
const N_ITEMS uint32 = 100000
/* District */
const N_DISTRICTS_PER_WAREHOUSE uint8 = 10
/* Customer */
const N_CUSTOMERS_PER_DISTRICT uint32 = 3000
/* NewOrder */
const N_INIT_NEW_ORDERS_PER_DISTRICT uint32 = 900
