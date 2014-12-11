// --------------------------------------------------------------------------------------------------------------------
// <copyright file="DbHelperTest.cs" company="OBeautifulCode">
//   Copyright 2014 OBeautifulCode
// </copyright>
// <summary>
//   Tests the <see cref="DbHelper"/> class.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace OBeautifulCode.Libs.Database.Test
{
    using System;
    using System.Collections.Generic;
    using System.Data;
    using System.Data.OleDb;
    using System.Data.SqlClient;
    using System.Data.SqlServerCe;
    using System.IO;
    using System.Linq;

    using OBeautifulCode.Libs.Reflection;

    using Xunit;

    /// <summary>
    /// Tests the <see cref="DbHelper"/> class.
    /// </summary>
    public class DbHelperTest : IDisposable
    {
        #region Fields (Private)

        #endregion

        #region Constructors

        /// <summary>
        /// Initializes a new instance of the <see cref="DbHelperTest"/> class.
        /// </summary>
        /// <remarks>
        /// Creates a fresh database of stock quotes and seeds with stock quote data.
        /// </remarks>
        public DbHelperTest()
        {
            this.DatabaseFilePath = CreatedSeededDatabase();
        }

        #endregion

        #region Properties

        /// <summary>
        /// Gets the database connection string.
        /// </summary>
        private string ConnectionString
        {
            get
            {
                return BuildConnectionString(this.DatabaseFilePath);
            }
        }

        /// <summary>
        /// Gets or sets the path to the database file.
        /// </summary>
        private string DatabaseFilePath { get; set; }

        #endregion

        #region Public Methods
        // ReSharper disable InconsistentNaming

        /// <summary>
        /// Dispose the instance.
        /// </summary>
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }

        /// <summary>
        /// Dispose the instance.
        /// </summary>
        /// <param name="disposing">Is disposing?</param>
        public virtual void Dispose(bool disposing)
        {
            if (disposing)
            {
                File.Delete(DatabaseFilePath);
            }
        }

        /// <summary>
        /// Test method.
        /// </summary>
        [Fact]
        public void OpenConnection_ConnetionStringIsNull_ThrowsArgumentNullException()
        {
            // Arrange, Act, Assert
            Assert.Throws<ArgumentNullException>(() => DbHelper.OpenConnection<SqlCeConnection>(null));
        }

        /// <summary>
        /// Test method.
        /// </summary>
        [Fact]
        public void OpenConnection_ConnetionStringIsWhitespace_ThrowsArgumentException()
        {
            // Arrange, Act, Assert
            Assert.Throws<ArgumentException>(() => DbHelper.OpenConnection<SqlCeConnection>(string.Empty));
            Assert.Throws<ArgumentException>(() => DbHelper.OpenConnection<SqlCeConnection>("  "));
            Assert.Throws<ArgumentException>(() => DbHelper.OpenConnection<SqlCeConnection>("  \r\n  "));
        }

        /// <summary>
        /// Test method.
        /// </summary>
        [Fact]
        public void OpenConnection_ConnetionStringNotProperlyConstructed_ThrowsArgumentException()
        {
            // Arrange, Act, Assert
            Assert.Throws<ArgumentException>(() => DbHelper.OpenConnection<SqlCeConnection>("connetionstring"));
        }

        /// <summary>
        /// Test method.
        /// </summary>
        [Fact]
        public static void OpenConnection_ConnectionCannotBeEstablished_ThrowsSqlException()
        {
            // Arrange, Act, Assert
            Assert.Throws<SqlCeException>(() => DbHelper.OpenConnection<SqlCeConnection>("Data Source=DOESNTEXIST;"));
        }

        /// <summary>
        /// Test method.
        /// </summary>
        [Fact]
        public void OpenConnection_ConnectionStringIsValidAndDatabaseIsAccessible_ReturnsOpenConnection()
        {
            // Arrange, Act, Assert
            using (var connection = DbHelper.OpenConnection<SqlCeConnection>(this.ConnectionString))
            {
                Assert.Equal(ConnectionState.Open, connection.State);
                connection.Close();
            }
        }

        /// <summary>
        /// Test method.
        /// </summary>
        [Fact]
        public void BuildCommand_ConnectionIsNull_ThrowsArgumentNullException()
        {
            // Arrange, Act, Assert
            Assert.Throws<ArgumentNullException>(() => DbHelper.BuildCommand(null, "Select * From [Whatever]"));
        }

        /// <summary>
        /// Test method.
        /// </summary>
        [Fact]
        public void BuildCommand_ConnectionIsNotOpen_ThrowsArgumentException()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlCeConnection>(this.ConnectionString);
            const string SqlStatement = "Select * From HistoricQuotes";
            connection.Close();

            // Act, Assert
            var actualException = Assert.Throws<ArgumentException>(() => DbHelper.BuildCommand(connection, SqlStatement));
            Assert.Equal("connection is in an invalid state: " + connection.State + ".  Must be Open.", actualException.Message);

            // Cleanup
            connection.Dispose();
        }

        /// <summary>
        /// Test method.
        /// </summary>
        [Fact]
        public void BuildCommand_CommandTextIsNull_ThrowsArgumentNullException()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlCeConnection>(this.ConnectionString);

            // Act, Assert
            Assert.Throws<ArgumentNullException>(() => DbHelper.BuildCommand(connection, null));

            // Cleanup
            connection.Dispose();
        }

        /// <summary>
        /// Test method.
        /// </summary>
        [Fact]
        public void BuildCommand_CommandTextIsWhitespace_ThrowsArgumentException()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlCeConnection>(this.ConnectionString);

            // Act, Assert
            Assert.Throws<ArgumentException>(() => DbHelper.BuildCommand(connection, string.Empty));
            Assert.Throws<ArgumentException>(() => DbHelper.BuildCommand(connection, "   "));
            Assert.Throws<ArgumentException>(() => DbHelper.BuildCommand(connection, "  \r\n  "));

            // Cleanup
            connection.Dispose();
        }

        /// <summary>
        /// Test method.
        /// </summary>
        [Fact]
        public void BuildCommand_TimeoutIsLessThanZero_ThrowsArgumentOutOfRangeException()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlCeConnection>(this.ConnectionString);
            const string SqlStatement = "Select * From HistoricQuotes";

            // Act, Assert
            Assert.Throws<ArgumentOutOfRangeException>(() => DbHelper.BuildCommand(connection, SqlStatement, null, CommandType.Text, null, false, -1));
            Assert.Throws<ArgumentOutOfRangeException>(() => DbHelper.BuildCommand(connection, SqlStatement, null, CommandType.Text, null, false, int.MinValue));

            // Cleanup            
            connection.Close();
            connection.Dispose();
        }

        /// <summary>
        /// Test method.
        /// </summary>
        [Fact]
        public void BuildCommand_TransactionIsInvalidHasBeenRolledBack_ThrowsArgumentException()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlCeConnection>(this.ConnectionString);
            const string SqlStatement = "Select * From HistoricQuotes";
            var transaction = connection.BeginTransaction();
            transaction.Rollback();

            // Act, Assert
            Assert.Null(transaction.Connection);
            var actualException = Assert.Throws<ArgumentException>(() => DbHelper.BuildCommand(connection, SqlStatement, null, CommandType.Text, transaction));
            Assert.Equal("transaction is invalid.", actualException.Message);

            // Cleanup            
            transaction.Dispose();
            connection.Close();
            connection.Dispose();
        }

        /// <summary>
        /// Test method.
        /// </summary>
        [Fact]
        public void BuildCommand_TransactionUsesDifferentConnectionThenTheSpecifiedOne_ThrowsArgumentException()
        {
            // Arrange
            var connection1 = DbHelper.OpenConnection<SqlCeConnection>(this.ConnectionString);
            string databaseFilePath2 = CreatedSeededDatabase();
            var connection2 = DbHelper.OpenConnection<SqlCeConnection>(BuildConnectionString(databaseFilePath2));
            var transaction = connection1.BeginTransaction();

            // Act, Assert
            var actualException = Assert.Throws<ArgumentException>(() => DbHelper.BuildCommand(connection2, "Select * From HistoricQuotes", null, CommandType.Text, transaction));
            Assert.Equal("transaction is using a different connection than the specified connection.", actualException.Message);

            // Cleanup
            transaction.Dispose();
            connection1.Close();
            connection1.Dispose();
            connection2.Close();
            connection2.Dispose();
        }

        /// <summary>
        /// Test method.
        /// </summary>
        [Fact]
        public void BuildCommand_UseParameterOfDifferentTypeThanOneSupportedByDataProvider_ThrowsInvalidOperationException()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlCeConnection>(this.ConnectionString);
            var parameter = new OleDbParameter("@symbol", OleDbType.VarChar, 10);
            var parameters = new IDataParameter[] { parameter };
            const string SqlStatement = "Select * From [HistoricQuotes] Where [Symbol] = @symbol";

            // Act, Assert
            var actualException = Assert.Throws<InvalidOperationException>(() => DbHelper.BuildCommand(connection, SqlStatement, parameters));
            Assert.Equal("Attempting to set a parameter of type OleDbParameter that was designed for a data provider other than the provider represented by the specified connection.", actualException.Message);

            // Cleanup
            connection.Close();
            connection.Dispose();
        }

        /// <summary>
        /// Test method.
        /// </summary>
        [Fact]
        public void BuildCommand_SomeParametersAreNull_NullParametersAreIgnored()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlCeConnection>(this.ConnectionString);
            var parameter = new SqlCeParameter("@symbol", SqlDbType.NVarChar, 15);
            var parameters = new IDataParameter[] { null, parameter, null, null };
            const string SqlStatement = "Select * From [HistoricQuotes] Where [Symbol] = @symbol";

            // Act, Assert
            Assert.DoesNotThrow(() => DbHelper.BuildCommand(connection, SqlStatement, parameters).Dispose());

            // Cleanup
            connection.Close();
            connection.Dispose();
        }

        /// <summary>
        /// Test method.
        /// </summary>
        [Fact]
        public void BuildCommand_ValidTextCommandAndNoTransaction_ReturnsExpectedData()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlCeConnection>(this.ConnectionString);
            const string CommandText = "Select [OpenPriceAdjust] From [HistoricQuotes] Where [Date]='1/5/2009' AND [Symbol]='msft'";

            // Act
            IDbCommand command = DbHelper.BuildCommand(connection, CommandText);
            object actual = command.ExecuteScalar();

            // Assert
            Assert.IsType<decimal>(actual);
            Assert.Equal(19.4519m, (decimal)actual);

            // Cleanup
            command.Dispose();
            connection.Close();
            connection.Dispose();
        }

        /// <summary>
        /// Test method.
        /// </summary>
        [Fact]
        public void BuildCommand_ValidTextCommandAndNoTransactionAndPrepareWithNoParameters_ReturnsExpectedData()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlCeConnection>(this.ConnectionString);
            const string CommandText = "Select [OpenPriceAdjust] From [HistoricQuotes] Where [Date]='1/5/2009' AND [Symbol]='msft'";

            // Act
            IDbCommand command = DbHelper.BuildCommand(connection, CommandText, null, CommandType.Text, null, true);
            object actual = command.ExecuteScalar();

            // Assert
            Assert.IsType<decimal>(actual);
            Assert.Equal(19.4519m, (decimal)actual);

            // Cleanup
            command.Dispose();
            connection.Close();
            connection.Dispose();
        }
        
        /// <summary>
        /// Test method.
        /// </summary>
        [Fact]
        public void BuildCommand_ValidTextCommandWithParameters_ReturnsExpectedData()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlCeConnection>(this.ConnectionString);
            const string CommandText = "Select [LowPriceAdjust] From [HistoricQuotes] Where [Date]=@date AND [Symbol]=@symbol";
            var dateParameter = new SqlParameter("@date", new DateTime(2009, 1, 5));
            var symbolParameter = new SqlParameter("@symbol", "msft");
            var parameters = new[] { dateParameter, symbolParameter };

            // Act
            IDbCommand command = DbHelper.BuildCommand(connection, CommandText, parameters);
            object actual = command.ExecuteScalar();

            // Assert
            Assert.IsType<decimal>(actual);
            Assert.Equal(19.317m, (decimal)actual);

            // Cleanup
            command.Dispose();
            connection.Close();
            connection.Dispose();
        }

        /// <summary>
        /// Test method.
        /// </summary>
        [Fact]
        public void BuildCommand_ValidTextCommandWithParametersAndPrepareCommand_ReturnsExpectedData()
        {
            // Arrange
            var connection = DbHelper.OpenConnection<SqlCeConnection>(this.ConnectionString);
            const string CommandText = "Select [ClosePriceAdjust] From [HistoricQuotes] Where [Date]=@date AND [Symbol]=@symbol";
            var dateParameter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 1, 5) };
            var symbolParameter = new SqlParameter("@symbol", SqlDbType.VarChar, 10) { Value = "msft" };
            var parameters = new[] { dateParameter, symbolParameter };

            // Act
            IDbCommand command = DbHelper.BuildCommand(connection, CommandText, parameters, CommandType.Text, null, true);
            object actual = command.ExecuteScalar();

            // Assert
            Assert.IsType<decimal>(actual);
            Assert.Equal(19.76m, (decimal)actual);

            // Cleanup
            command.Dispose();
            connection.Close();
            connection.Dispose();
        }


 
        //    // valid SP command.  No transaction
        //    using (var sqlConnection = DbHelper.OpenConnection<SqlCeConnection>(ConnectionString))
        //    {

        //        // parameters, don't prepare
        //        dateParameter = new SqlParameter("@date", new DateTime(2009, 1, 5));
        //        var symbolParameter = new SqlParameter("@symbol", "msft");
        //        var closePriceOutputParameter = new SqlParameter { ParameterName = "@closepriceadjust", Direction = ParameterDirection.Output, SqlDbType = SqlDbType.Money };
        //        using (IDbCommand command = DbHelper.BuildCommand(sqlConnection, null, CommandType.StoredProcedure, "FetchClosePrice", new[] { dateParameter, symbolParameter, closePriceOutputParameter }, false, 30))
        //        {
        //            command.ExecuteScalar();
        //            Assert.IsType<decimal>(closePriceOutputParameter.Value);
        //            Assert.Equal(19.76m, (decimal)closePriceOutputParameter.Value);
        //        }

        //        // parameters, prepare
        //        dateParameter = new SqlParameter("@date", SqlDbType.SmallDateTime) { Value = new DateTime(2009, 2, 5) };
        //        symbolParameter = new SqlParameter("@symbol", SqlDbType.VarChar, 10) { Value = "msft" };
        //        closePriceOutputParameter = new SqlParameter { ParameterName = "@closepriceadjust", Direction = ParameterDirection.Output, SqlDbType = SqlDbType.Money };
        //        using (IDbCommand command = DbHelper.BuildCommand(sqlConnection, null, CommandType.StoredProcedure, "FetchClosePrice", new[] { dateParameter, symbolParameter, closePriceOutputParameter }, true, 30))
        //        {
        //            command.ExecuteScalar();
        //            Assert.IsType<decimal>(closePriceOutputParameter.Value);
        //            Assert.Equal(18.34m, (decimal)closePriceOutputParameter.Value);
        //        }

        //    }

        //    // todo: test CommandType = TableDirect

        //    // valid command with transaction - rollback transaction
        //    DateTime now = DateTime.Now;
        //    const string insertSql = "Insert Into [DbHelper] (Date, Value) Values (@Date, @Value)";
        //    const string verifyValueSql = "Select [Value] From [DbHelper] Where Date = @Date";
        //    using (var sqlConnection = DbHelper.OpenConnection<SqlCeConnection>(ConnectionString))
        //    {

        //        // insert value within transaciton, verify value added, rollback
        //        using (var sqlTransaction = sqlConnection.BeginTransaction())
        //        {
        //            dateParameter = new SqlParameter("@Date", SqlDbType.DateTime) { Value = now };
        //            var valueParameter = new SqlParameter("@Value", SqlDbType.Decimal) { Value = 13.12, Precision = 10, Scale = 5 };
        //            using (IDbCommand command = DbHelper.BuildCommand(sqlConnection, sqlTransaction, CommandType.Text, insertSql, new[] { dateParameter, valueParameter }, true, 30))
        //            {
        //                Assert.Equal(1, command.ExecuteNonQuery());
        //            }

        //            dateParameter = new SqlParameter("@Date", SqlDbType.DateTime) { Value = now };
        //            using (IDbCommand command = DbHelper.BuildCommand(sqlConnection, sqlTransaction, CommandType.Text, verifyValueSql, new[] { dateParameter }, true, 30))
        //            {
        //                object result = command.ExecuteScalar();
        //                Assert.IsType<decimal>(result);
        //                Assert.Equal(13.12m, (decimal)result);
        //            }
        //            sqlTransaction.Rollback();
        //        }

        //        // verify value was discarded because of rollback
        //        dateParameter = new SqlParameter("@Date", SqlDbType.DateTime) { Value = now };
        //        using (IDbCommand command = DbHelper.BuildCommand(sqlConnection, null, CommandType.Text, verifyValueSql, new[] { dateParameter }, true, 30))
        //        {
        //            Assert.Null(command.ExecuteScalar());
        //        }
        //    }

        //    // valid command with transaction - commit transaction
        //    Thread.Sleep(1000);
        //    now = DateTime.Now;
        //    using (var sqlConnection = DbHelper.OpenConnection<SqlCeConnection>(ConnectionString))
        //    {

        //        // insert value within transaciton, verify value added, rollback
        //        using (var sqlTransaction = sqlConnection.BeginTransaction())
        //        {
        //            dateParameter = new SqlParameter("@Date", SqlDbType.DateTime) { Value = now };
        //            var valueParameter = new SqlParameter("@Value", SqlDbType.Decimal) { Value = 13.12, Precision = 10, Scale = 5 };
        //            using (IDbCommand command = DbHelper.BuildCommand(sqlConnection, sqlTransaction, CommandType.Text, insertSql, new[] { dateParameter, valueParameter }, true, 30))
        //            {
        //                Assert.Equal(1, command.ExecuteNonQuery());
        //            }

        //            dateParameter = new SqlParameter("@Date", SqlDbType.DateTime) { Value = now };
        //            using (IDbCommand command = DbHelper.BuildCommand(sqlConnection, sqlTransaction, CommandType.Text, verifyValueSql, new[] { dateParameter }, true, 30))
        //            {
        //                object result = command.ExecuteScalar();
        //                Assert.IsType<decimal>(result);
        //                Assert.Equal(13.12m, (decimal)result);
        //            }
        //            sqlTransaction.Commit();
        //        }

        //        dateParameter = new SqlParameter("@Date", SqlDbType.DateTime) { Value = now };
        //        using (IDbCommand command = DbHelper.BuildCommand(sqlConnection, null, CommandType.Text, verifyValueSql, new[] { dateParameter }, true, 30))
        //        {
        //            object result = command.ExecuteScalar();
        //            Assert.IsType<decimal>(result);
        //            Assert.Equal(13.12m, (decimal)result);
        //        }
        //    }


        //    // valid command with ambient transaction - rollback
        //    Thread.Sleep(1000);
        //    now = DateTime.Now;
        //    using (new TransactionScope())
        //    {
        //        using (var sqlConnection = DbHelper.OpenConnection<SqlCeConnection>(ConnectionString))
        //        {

        //            // insert value
        //            dateParameter = new SqlParameter("@Date", SqlDbType.DateTime) { Value = now };
        //            var valueParameter = new SqlParameter("@Value", SqlDbType.Decimal) { Value = 15.4, Precision = 10, Scale = 5 };
        //            using (IDbCommand command = DbHelper.BuildCommand(sqlConnection, null, CommandType.Text, insertSql, new[] { dateParameter, valueParameter }, true, 30))
        //            {
        //                Assert.Equal(1, command.ExecuteNonQuery());
        //            }

        //        }
        //        using (var sqlConnection = DbHelper.OpenConnection<SqlCeConnection>(ConnectionString))
        //        {
        //            // verify exists
        //            dateParameter = new SqlParameter("@Date", SqlDbType.DateTime) { Value = now };
        //            using (IDbCommand command = DbHelper.BuildCommand(sqlConnection, null, CommandType.Text, verifyValueSql, new[] { dateParameter }, true, 30))
        //            {
        //                object result = command.ExecuteScalar();
        //                Assert.IsType<decimal>(result);
        //                Assert.Equal(15.4m, (decimal)result);
        //            }
        //        }
        //    }
        //    using (var sqlConnection = DbHelper.OpenConnection<SqlCeConnection>(ConnectionString))
        //    {
        //        // scope was not comitted - data should have been discarded
        //        dateParameter = new SqlParameter("@Date", SqlDbType.DateTime) { Value = now };
        //        using (IDbCommand command = DbHelper.BuildCommand(sqlConnection, null, CommandType.Text, verifyValueSql, new[] { dateParameter }, true, 30))
        //        {
        //            Assert.Null(command.ExecuteScalar());
        //        }
        //    }


        //    // valid command with ambient transaction - commit
        //    Thread.Sleep(1000);
        //    now = DateTime.Now;
        //    using (var scope = new TransactionScope())
        //    {
        //        using (var sqlConnection = DbHelper.OpenConnection<SqlCeConnection>(ConnectionString))
        //        {

        //            // insert value
        //            dateParameter = new SqlParameter("@Date", SqlDbType.DateTime) { Value = now };
        //            var valueParameter = new SqlParameter("@Value", SqlDbType.Decimal) { Value = 15.4, Precision = 10, Scale = 5 };
        //            using (IDbCommand command = DbHelper.BuildCommand(sqlConnection, null, CommandType.Text, insertSql, new[] { dateParameter, valueParameter }, true, 30))
        //            {
        //                Assert.Equal(1, command.ExecuteNonQuery());
        //            }

        //        }
        //        using (var sqlConnection = DbHelper.OpenConnection<SqlCeConnection>(ConnectionString))
        //        {
        //            // verify exists
        //            dateParameter = new SqlParameter("@Date", SqlDbType.DateTime) { Value = now };
        //            using (IDbCommand command = DbHelper.BuildCommand(sqlConnection, null, CommandType.Text, verifyValueSql, new[] { dateParameter }, true, 30))
        //            {
        //                object result = command.ExecuteScalar();
        //                Assert.IsType<decimal>(result);
        //                Assert.Equal(15.4m, (decimal)result);
        //            }
        //        }
        //        scope.Complete();
        //    }
        //    // verify exists
        //    using (var sqlConnection = DbHelper.OpenConnection<SqlCeConnection>(ConnectionString))
        //    {

        //        dateParameter = new SqlParameter("@Date", SqlDbType.DateTime) { Value = now };
        //        using (IDbCommand command = DbHelper.BuildCommand(sqlConnection, null, CommandType.Text, verifyValueSql, new[] { dateParameter }, true, 30))
        //        {
        //            object result = command.ExecuteScalar();
        //            Assert.IsType<decimal>(result);
        //            Assert.Equal(15.4m, (decimal)result);
        //        }
        //    }


        //    //    // transaction not provided - connection is pending a local transaction
        //    //    using ( SqlCeConnection connection = DbHelper.OpenSqlCeConnection( ConnectionString ) )
        //    //    {
        //    //        using ( connection.BeginTransaction( IsolationLevel.ReadUncommitted ) )
        //    //        {
        //    //            Assert.Throws<InvalidOperationException>( () => DbHelper.BuildDataReader( connection , sqlQuery , 30 ) );
        //    //        }
        //    //    }


        //}



        // ReSharper restore InconsistentNaming
        #endregion

        #region Internal Methods

        #endregion

        #region Protected Methods

        #endregion

        #region Private Methods

        /// <summary>
        /// Creates a database of stock quote data and seeds it.
        /// </summary>
        /// <returns>
        /// Returns a path to the database.
        /// </returns>
        private string CreatedSeededDatabase()
        {
            // create the database file
            string databaseFilePath = Path.GetTempFileName();
            File.Create(databaseFilePath).Dispose();

            // load the schema
            string schemaSql = AssemblyHelper.ReadEmbeddedResourceAsString("StockQuotesSchema.sql", true);
            List<string> schemaSqlStatements = SqlBatchStatementSplitter.SplitSqlAndRemoveEmptyStatements(schemaSql).ToList();

            // load the data
            string dataSql = AssemblyHelper.ReadEmbeddedResourceAsString("StockQuotesData.sql", true);
            List<string> dataSqlStatements = dataSql.Split(new[] { Environment.NewLine }, StringSplitOptions.None).Where(sql => !string.IsNullOrWhiteSpace(sql)).ToList();

            // import schema and data
            List<string> allSqlStatements = schemaSqlStatements.Concat(dataSqlStatements).ToList();
            using (var connection = new SqlCeConnection(BuildConnectionString(databaseFilePath)))
            {
                connection.Open();
                foreach (var sqlStatement in allSqlStatements)
                {
                    using (var command = new SqlCeCommand(sqlStatement, connection))
                    {
                        command.ExecuteNonQuery();
                    }
                }
                connection.Close();
            }

            return databaseFilePath;
        }

        /// <summary>
        /// Builds a connection string.
        /// </summary>
        /// <param name="databaseFilePath">The path to the database.</param>
        /// <returns>
        /// The connection string.
        /// </returns>
        private string BuildConnectionString(string databaseFilePath)
        {
            return "Data Source=" + databaseFilePath;
        }

        #endregion

    }
}
