using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using SchemaZen.Library.Models;

namespace SchemaZen.Library.Command {
	public class CompareCommand : BaseCommand {
		private const string PREFIX_DIR = "dir:";
		private bool _dropSourceDb = false;
		private bool _dropTargetDb = false;

		public string Source { get; set; }
		public string Target { get; set; }
		public bool Verbose { get; set; }
		public string OutDiff { get; set; }

		public bool Execute() {
			var sourceDb = new Database();
			var targetDb = new Database();
			if (Source.StartsWith(PREFIX_DIR)) {
				sourceDb.Connection = parseConnection(Source);
				_dropSourceDb = true;
			} else {
				sourceDb.Connection = Source;
			}
			if (Target.StartsWith(PREFIX_DIR)) {
				targetDb.Connection = parseConnection(Target);
				_dropTargetDb = true;
			} else {
				targetDb.Connection = Target;
			}
			try {
				sourceDb.Load();
				targetDb.Load();
				var diff = sourceDb.Compare(targetDb);
				if (diff.IsDiff) {
					Console.WriteLine("Databases are different.");
					Console.WriteLine(diff.SummarizeChanges(Verbose));
					if (!string.IsNullOrEmpty(OutDiff)) {
						Console.WriteLine();
						if (!Overwrite && File.Exists(OutDiff)) {
							var message = $"{OutDiff} already exists - set overwrite to true if you want to delete it";
							throw new InvalidOperationException(message);
						}
						File.WriteAllText(OutDiff, diff.Script());
						Console.WriteLine($"Script to make the databases identical has been created at {Path.GetFullPath(OutDiff)}");
					}
					return true;
				}
				Console.WriteLine("Databases are identical.");
			} finally {
				if (_dropSourceDb) {
					DBHelper.DropDb(sourceDb.Connection);
				}
				if (_dropTargetDb) {
					DBHelper.DropDb(targetDb.Connection);
				}
			}

			return false;
		}

		protected String parseConnection(String value) {
			var path = Path.GetFullPath(value.Substring(PREFIX_DIR.Length));
			string dbName = Guid.NewGuid().ToString();
			string connectionString = $"server=(LocalDB)\\MSSQLLocalDB;database={dbName};Trusted_Connection=yes;";

			var createCommand = new CreateCommand() {
				ConnectionString = connectionString,
				Logger = Logger,
				Overwrite = true,
				ScriptDir = path
			};

			try {
				createCommand.Execute(null);
			} catch (BatchSqlFileException ex) {
				Logger.Log(TraceLevel.Info, $"{Environment.NewLine}Create completed with the following errors:");
				foreach (var e in ex.Exceptions) {
					Logger.Log(TraceLevel.Info, $"- {e.FileName.Replace("/", "\\")} (Line {e.LineNumber}):");
					Logger.Log(TraceLevel.Error, $" {e.Message}");
				}
				throw ex;
			} catch (SqlFileException ex) {
				Logger.Log(TraceLevel.Info, $@"{Environment.NewLine}An unexpected SQL error occurred while executing scripts, and the process wasn't completed.
{ex.FileName.Replace("/", "\\")} (Line {ex.LineNumber}):");
				Logger.Log(TraceLevel.Error, ex.Message);
				throw ex;
			}

			return connectionString;
		}
	}
}
