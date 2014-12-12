Create Procedure FetchClosePrice( @Symbol nvarchar(10),@Date datetime,@Close money output )
As
  Set nocount On;
  Declare @Result money
  Select  @Close = [Close]
  From    [StockQuotes]
  Where   ( [Symbol] = @Symbol ) And
          ( [Date] = @Date )
