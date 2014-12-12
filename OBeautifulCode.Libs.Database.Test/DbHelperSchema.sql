Create Table [dbo].[DbHelper]
  (
     [Id]       [int] Identity(1, 1) Not Null,
     [Date]     [datetime] Not Null,
     [Value]    [decimal]( 10, 5 ) Null,
     [Csv,Test] [varchar]( 50 ) Null,
     Constraint [PK_DbHelper] Primary Key Clustered ( [Id] Asc )With (pad_index = Off, statistics_norecompute = Off, ignore_dup_key = Off, allow_row_locks = on, allow_page_locks = on) On [PRIMARY]
  )
On [PRIMARY]
