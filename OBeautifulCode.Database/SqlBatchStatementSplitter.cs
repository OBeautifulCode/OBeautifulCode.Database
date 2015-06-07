// --------------------------------------------------------------------------------------------------------------------
// <copyright file="SqlBatchStatementSplitter.cs" company="OBeautifulCode">
//   Copyright 2014 OBeautifulCode
// </copyright>
// <summary>
//   Splits an sql batch statement into individual statements.
// </summary>
// --------------------------------------------------------------------------------------------------------------------

namespace OBeautifulCode.Database
{
    using System.Collections.Generic;
    using System.Linq;
    using System.Text.RegularExpressions;

    /// <summary>
    /// Splits an SQL batch statement into individual statements.
    /// </summary>
    /// <remarks>
    /// Adapted from <a href="http://blog.stpworks.com/archive/2010/02/22/how-to-split-sql-file-by-go-statement.aspx"/>
    /// Go statement must be on its own line.  Does not work for semicolon separator.
    /// </remarks>
    public class SqlBatchStatementSplitter
    {
        /// <summary>
        /// Regex pattern to split up an SQL statement
        /// </summary>
        private const string SqlStatementSeparatorRegexPattern = @"^\s*GO\s*$";

        /// <summary>
        /// Splits a batch SQL statement into individual statements.
        /// </summary>
        /// <param name="batchSql">The batch SQL to process.</param>
        /// <returns>Returns an enumerable with individual SQL statements.</returns>
        public static IEnumerable<string> SplitSqlAndRemoveEmptyStatements(string batchSql)
        {
            return Regex.Split(batchSql + "\n", SqlStatementSeparatorRegexPattern, RegexOptions.IgnoreCase | RegexOptions.Multiline).Where(statement => !string.IsNullOrWhiteSpace(statement));
        }
    }
}
