﻿--------------------------------------------------------------------------------------------------------------------
-- <copyright file="StockQuotes.sql" company="OBeautifulCode">
--    Copyright 2014 OBeautifulCode
-- </copyright>
-- <summary>
--    Stores stock quote data
-- </summary>
--------------------------------------------------------------------------------------------------------------------

/*********************************** Table *************************************/

Create Table [dbo].[StockQuotes]
  (
     [Symbol]       [nvarchar](15)  Not Null,
     [Date]         [datetime]      Not Null,
     [Open]         [money]         Not Null,
     [High]         [money]         Not Null,
     [Low]          [money]         Not Null,
     [Close]        [money]         Not Null,
     [Volume]       [bigint]        Not Null,     
     [OpenInterest] [decimal](9,3)  Null     
  )

/********************************** Indices ************************************/

Create NonClustered Index [IX_StockQuotes_Symbol]
  On [StockQuotes] ( [Symbol] Asc )  

Create NonClustered Index [IX_StockQuotes_Date]
  On [StockQuotes] ( [Date] Asc )

Create Unique NonClustered Index [IX_StockQuotes_SymbolDate]
  On [StockQuotes] ( [Symbol] Asc, [Date] Asc )