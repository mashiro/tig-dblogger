using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Threading;
using Misuzilla.Net.Irc;
using Misuzilla.Applications.TwitterIrcGateway;
using Misuzilla.Applications.TwitterIrcGateway.AddIns;
using Misuzilla.Applications.TwitterIrcGateway.AddIns.Console;
using Misuzilla.Applications.TwitterIrcGateway.AddIns.TypableMap;
using MySql.Data.MySqlClient;

namespace Spica.Applications.TwitterIrcGateway.AddIns.DBLogger
{
	/// <summary>
	/// DBLoggerのテーブルに保存されるステータスの型
	/// </summary>
	public class DBLoggerStatus
	{
		public Int64 StatusId { get; set; }
		public DateTime CreatedAt { get; set; }
		public String ScreenName { get; set; }
		public String Text { get; set; }
	}

	/// <summary>
	/// DBLoggerの設定
	/// </summary>
	public class DBLoggerConfiguration : IConfiguration
	{
		public String TableName { get; set; }
		public String ChannelName { get; set; }
		public String DateTimeFormat { get; set; }
		public Int32 SelectCount { get; set; }
		public Boolean LoggingOnStartUp { get; set; }
		public Int32 IntervalInsert { get; set; }

		public String Timeout { get; set; }
		public String Server { get; set; }
		public String Port { get; set; }
		public String CharSet { get; set; }
		public String Database { get; set; }
		public String Username { get; set; }
		public String Password { get; set; }

		[Browsable(false)]
		public List<UserDefinedQuery> UserDefinedQueryList { get; set; }

		[Browsable(false)]
		public String ConnectionString
		{
			get
			{
				Action<StringBuilder, String, String> appendParameter =
					(builder, name, value) => { if (!String.IsNullOrEmpty(value)) { builder.AppendFormat("{0}={1};", name, value); } };

				StringBuilder sb = new StringBuilder();
				appendParameter(sb, "Connect Timeout", Timeout);
				appendParameter(sb, "Server", Server);
				appendParameter(sb, "Port", Port);
				appendParameter(sb, "CharSet", CharSet);
				appendParameter(sb, "Database", Database);
				appendParameter(sb, "Username", Username);
				appendParameter(sb, "Password", Password);

				return sb.ToString();
			}
		}

		public DBLoggerConfiguration()
		{
			TableName = String.Empty;
			ChannelName = "#DBLogger";
			DateTimeFormat = "yyyy/MM/dd HH:mm:ss";
			SelectCount = 10;
			LoggingOnStartUp = false;
			IntervalInsert = 30;

			Timeout = String.Empty;
			Server = String.Empty;
			Port = String.Empty;
			CharSet = String.Empty;
			Database = String.Empty;
			Username = String.Empty;
			Password = String.Empty;

			UserDefinedQueryList = new List<UserDefinedQuery>();
		}
	}

	/// <summary>
	/// DBLoggerコンテキスト
	/// </summary>
	[Description("ログデータベースの設定を行うコンテキストに切り替えます")]
	public class DBLoggerContext : Context
	{
		private DBLoggerAddIn AddIn { get { return CurrentSession.AddInManager.GetAddIn<DBLoggerAddIn>(); } }
		private DBLoggerConfiguration Config { get { return AddIn.Config; } }

		public override IConfiguration[] Configurations { get { return new IConfiguration[] { Config }; } }
		protected override void OnConfigurationChanged(IConfiguration config, System.Reflection.MemberInfo memberInfo, object value)
		{
			if (config is DBLoggerConfiguration)
			{
				AddIn.Config = config as DBLoggerConfiguration;
				CurrentSession.AddInManager.SaveConfig(AddIn.Config);
			}
		}

		/// <summary>
		/// コンソールでコマンドを解釈する前に実行する処理
		/// </summary>
		[Browsable(false)]
		public override Boolean OnPreProcessInput(String inputLine)
		{
			if (CurrentSession.Config.EnableTypableMap && AddIn.TypableMapCommand != null)
			{
				// コンテキスト名を求める
				StringBuilder sb = new StringBuilder();
				foreach (Context ctx in Console.ContextStack) sb.Insert(0, ctx.ContextName.Replace("Context", "") + "\\");
				sb.Append(Console.CurrentContext.ContextName.Replace("Context", ""));

				// PRIVを作成
				PrivMsgMessage priv = new PrivMsgMessage(Console.ConsoleChannelName, inputLine);
				priv.SenderNick = sb.ToString();
				priv.SenderHost = "twitter@" + Server.ServerName;

				// TypableMapCommandProcessorで処理
				if (AddIn.TypableMapCommand.Process(priv))
				{
					return true;
				}
			}

			return false;
		}

		[Description("ユーザ定義クエリの設定を行うコンテキストに切り替えます")]
		public void UserDefinedQuery()
		{
			Type type = typeof(UserDefinedQueryContext);
			UserDefinedQueryContext ctx = Console.GetContext(type, CurrentServer, CurrentSession) as UserDefinedQueryContext;
			Console.PushContext(ctx);
		}

		[Description("データベースにDBLoggerが使用するテーブルを作成します")]
		public void CreateDBLoggerTable()
		{
			DBLoggerQueryExecuter executer = new DBLoggerQueryExecuter(CurrentSession, Console);
			executer.ExceptionHandler(() =>
			{
				executer.Execute(() => AddIn.CreateDBLoggerTable());
			});
		}

		[Description("ロギングを開始します")]
		public void StartLogging()
		{
			AddIn.StartLogging();
			Console.NotifyMessage("ロギングを開始しました。");
		}

		[Description("ロギングを終了します")]
		public void EndLogging()
		{
			AddIn.EndLogging();
			Console.NotifyMessage("ロギングを終了しました。");
		}

		[Description("指定したクエリを実行します")]
		public void ExecuteReader(String query)
		{
			DBLoggerQueryExecuter executer = new DBLoggerQueryExecuter(CurrentSession, Console);
			executer.ExceptionHandler(() =>
			{
				executer.Execute(() =>
				{
					AddIn.CreateConnection((con) =>
					{
						MySqlCommand command = new MySqlCommand(query, con);
						executer.ShowStatuses(AddIn.ExecuteReader(command));
						return GetSelectRangeString();
					});
				});
			});
		}

		[Description("指定したクエリを実行し単一の値を取得します")]
		public void ExecuteScalar(String query)
		{
			DBLoggerQueryExecuter executer = new DBLoggerQueryExecuter(CurrentSession, Console);
			executer.ExceptionHandler(() =>
			{
				executer.Execute(() =>
				{
					AddIn.CreateConnection((con) =>
					{
						MySqlCommand command = new MySqlCommand(query, con);
						Console.NotifyMessage(AddIn.ExecuteScalar(command).ToString());
					});
				});
			});
		}

		[Description("指定したクエリを実行し影響される行の数を返します")]
		public void ExecuteNonQuery(String query)
		{
			DBLoggerQueryExecuter executer = new DBLoggerQueryExecuter(CurrentSession, Console);
			executer.ExceptionHandler(() =>
			{
				executer.Execute(() =>
				{
					AddIn.CreateConnection((con) =>
					{
						MySqlCommand command = new MySqlCommand(query, con);
						Console.NotifyMessage(AddIn.ExecuteNonQuery(command).ToString());
					});
				});
			});
		}

		[Description("指定したWHERE句でタイムラインを検索します")]
		public void Select(String where)
		{
			DBLoggerQueryExecuter executer = new DBLoggerQueryExecuter(CurrentSession, Console);
			executer.ExceptionHandler(() =>
			{
				SelectImpl(executer, where);
			});
		}

		[Description("指定したユーザのタイムラインを検索します")]
		public void SelectByScreenName(String[] screenNames)
		{
			DBLoggerQueryExecuter executer = new DBLoggerQueryExecuter(CurrentSession, Console);
			executer.ExceptionHandler(() =>
			{
				executer.Execute(() =>
				{
					// クエリを作成
					StringBuilder sb = new StringBuilder();
					sb.Append("screen_name in (");
					for (int i = 0; i < screenNames.Length; ++i)
					{
						if (i != 0) sb.Append(", ");
						sb.AppendFormat("'{0}'", screenNames[i]);
					}
					sb.Append(")");

					// 実行
					executer.ShowStatuses(AddIn.Select(sb.ToString()));
					return GetSelectRangeString();
				});
			});
		}

		[Description("指定したテキストでタイムラインを検索します")]
		public void SelectByText(String text)
		{
			DBLoggerQueryExecuter executer = new DBLoggerQueryExecuter(CurrentSession, Console);
			executer.ExceptionHandler(() =>
			{
				executer.Execute(() =>
				{
					String where = String.Format("text like \"%{0}%\"", text);
					executer.ShowStatuses(AddIn.Select(where));
					return GetSelectRangeString();
				});
			});
		}

		[Description("指定したユーザ定義クエリでタイムラインを検索します")]
		public void SelectByUserDefinedQuery(String[] args)
		{
			try
			{
				UserDefinedQuery query = FindAt(Config.UserDefinedQueryList, args[0]);
				String where = String.Format(query.Query, args.Where((s, i) => i > 0).ToArray());

				DBLoggerQueryExecuter executer = new DBLoggerQueryExecuter(CurrentSession, Console);
				executer.ExceptionHandler(() =>
				{
					SelectImpl(executer, where);
				});
			}
			catch (Exception ex)
			{
				Console.NotifyMessage(ex.Message);
			}
		}

		[Description("次のページを検索します")]
		public void NextPage()
		{
			DBLoggerQueryExecuter executer = new DBLoggerQueryExecuter(CurrentSession, Console);
			executer.ExceptionHandler(() =>
			{
				executer.Execute(() =>
				{
					executer.ShowStatuses(AddIn.SelectNextPage());
					return GetSelectRangeString();
				});
			});
		}

		[Description("前のページを検索します")]
		public void PrevPage()
		{
			DBLoggerQueryExecuter executer = new DBLoggerQueryExecuter(CurrentSession, Console);
			executer.ExceptionHandler(() =>
			{
				executer.Execute(() =>
				{
					executer.ShowStatuses(AddIn.SelectPrevPage());
					return GetSelectRangeString();
				});
			});
		}

		[Description("データベースに保存されているステータス数を取得します")]
		public void StatusCount()
		{
			DBLoggerQueryExecuter executer = new DBLoggerQueryExecuter(CurrentSession, Console);
			executer.ExceptionHandler(() =>
			{
				executer.Execute(() =>
				{
					AddIn.CreateConnection((con) =>
					{
						MySqlCommand command = new MySqlCommand(String.Format("select count(status_id) from {0}", Config.TableName), con);
						Console.NotifyMessage(String.Format("現在 {0:N0} 件のステータスが保存されています。", (Int64)AddIn.ExecuteScalar(command)));
					});
				});
			});
		}

		#region Private
		/// <summary>
		/// 検索範囲を文字列で取得する
		/// </summary>
		private String GetSelectRangeString()
		{
			return String.Format("{0} - {1} 件目", AddIn.SelectRangeStart, AddIn.SelectRangeEnd);
		}

		/// <summary>
		/// 例外ハンドリングせずにSelect
		/// </summary>
		private void SelectImpl(DBLoggerQueryExecuter executer, String where)
		{
			executer.Execute(() =>
			{
				executer.ShowStatuses(AddIn.Select(where));
				return GetSelectRangeString();
			});
		}

		/// <summary>
		/// インデックスをパースしつつ配列から取得
		/// </summary>
		private T FindAt<T>(IList<T> source, String index)
		{
			try
			{
				return source[Int32.Parse(index)];
			}
			catch
			{
				throw new Exception("クエリの指定が正しくありません。");
			}
		}
		#endregion
	}

	/// <summary>
	/// DBLoggerアドイン
	/// </summary>
	public class DBLoggerAddIn : AddInBase
	{
		/// <summary>
		/// TypableMap
		/// </summary>
		public TypableMapCommandProcessor TypableMapCommand { get; private set; }

		/// <summary>
		/// 専用コンソール
		/// </summary>
		public Misuzilla.Applications.TwitterIrcGateway.AddIns.Console.Console Console { get; private set; }

		/// <summary>
		/// 設定
		/// </summary>
		public DBLoggerConfiguration Config { get; internal set; }

		/// <summary>
		/// 最後に実行したクエリのWhere句
		/// </summary>
		public String SelectLastWhere { get; private set; }

		/// <summary>
		/// ページ
		/// </summary>
		public Int32 SelectPage { get; private set; }

		/// <summary>
		/// 件数範囲の開始を取得
		/// </summary>
		public Int32 SelectRangeStart { get { return SelectPage * Config.SelectCount + 1; } }

		/// <summary>
		/// 件数範囲の終端を取得
		/// </summary>
		public Int32 SelectRangeEnd { get { return (SelectPage + 1) * Config.SelectCount; } }

		private Thread _insertThread = null;
		private EventWaitHandle _insertThreadEvent = null;
		private Boolean _insertThreadExitFlag = false;
		private Queue<DBLoggerStatus> _insertQueue = new Queue<DBLoggerStatus>();


		/// <summary>
		/// コンストラクタ
		/// </summary>
		public DBLoggerAddIn()
		{
			Console = new Misuzilla.Applications.TwitterIrcGateway.AddIns.Console.Console();
			SelectLastWhere = String.Empty;
			SelectPage = 0;
		}

		#region Utility
		/// <summary>
		/// Select系クエリのプリフィックスを取得します。
		/// </summary>
		public String GetSelectQueryPrefix()
		{
			return String.Format("select * from {0} where ", Config.TableName);
		}

		/// <summary>
		/// Select系クエリのサフィックスを取得します。
		/// </summary>
		public String GetSelectQuerySuffix(Int32 page)
		{
			return String.Format(" order by created_at desc limit {0}, {1}", page * Config.SelectCount, Config.SelectCount);
		}

		/// <summary>
		/// Select系クエリを作成します。
		/// </summary>
		public String MakeSelectQuery(String where, Int32 page)
		{
			StringBuilder sb = new StringBuilder();
			sb.Append(GetSelectQueryPrefix());
			sb.Append(where);
			sb.Append(GetSelectQuerySuffix(page));
			return sb.ToString();
		}

		/// <summary>
		/// コンソールにNOTICEで送信する
		/// </summary>
		public void NotifyMessage(String message)
		{
			foreach (var console in GetConsoles())
			{
				console.NotifyMessage(message);
			}
		}

		/// <summary>
		/// Consoleのリストを取得する
		/// </summary>
		public IEnumerable<Misuzilla.Applications.TwitterIrcGateway.AddIns.Console.Console> GetConsoles()
		{
			yield return CurrentSession.AddInManager.GetAddIn<ConsoleAddIn>();
			yield return this.Console;
		}

		/// <summary>
		/// 例外ハンドラ
		/// </summary>
		private void ExceptionHandler(Action action)
		{
			try
			{
				action();
			}
			catch (Exception ex)
			{
				NotifyMessage(ex.Message);
			}
		}

		/// <summary>
		/// インデックス付きForEach
		/// </summary>
		private void ForEach<T>(IEnumerable<T> source, Action<T, Int32> action)
		{
			Int32 i = 0;
			foreach (T arg in source)
			{
				action(arg, i);
				++i;
			}
		}

		#endregion

		#region Event
		/// <summary>
		/// アドイン初期化時
		/// </summary>
		public override void Initialize()
		{
			// 設定を取得
			Config = CurrentSession.AddInManager.GetConfig<DBLoggerConfiguration>();

			// コンソールを有効に
			Console.Attach(Config.ChannelName, CurrentServer, CurrentSession, typeof(DBLoggerContext), false);

			CurrentSession.SessionStarted += (sender, e) =>
			{
				if (String.IsNullOrEmpty(Config.TableName))
				{
					// メールアドレスが来ることもあるので英数以外をアンダースコアに置き換える。
					String replaced = Regex.Replace(e.UserName.ToLower(), "\\W", "_");

					// デフォルトのテーブル名を設定
					Config.TableName = String.Format("twitter_{0}", replaced);
				}
			};

			CurrentSession.AddInsLoadCompleted += (sender, e) =>
			{
				// イベントを登録
				CurrentSession.AddInManager.GetAddIn<ConsoleAddIn>().RegisterContext<DBLoggerContext>();
				CurrentSession.PostMessageReceived += new EventHandler<MessageReceivedEventArgs>(CurrentSession_PostMessageReceived);
				CurrentSession.PreFilterProcessTimelineStatus += new EventHandler<TimelineStatusEventArgs>(CurrentSession_PreFilterProcessTimelineStatus);
				CurrentSession.PostSendUpdateStatus += new EventHandler<StatusUpdateEventArgs>(CurrentSession_PostSendUpdateStatus);

				// TypableMapコマンドを取得
				TypableMapCommand = CurrentSession.AddInManager.GetAddIn<TypableMapSupport>().TypableMapCommands;

				if (Config.LoggingOnStartUp)
				{
					StartLogging();
				}
			};
		}

		/// <summary>
		/// アドイン破棄時
		/// </summary>
		public override void Uninitialize()
		{
			EndLogging();
			Console.Detach();

			base.Uninitialize();
		}

		/// <summary>
		/// IRCメッセージ受信時、TwitterIrcGatewayが処理した後のイベント
		/// </summary>
		private void CurrentSession_PostMessageReceived(object sender, MessageReceivedEventArgs e)
		{
			JoinMessage joinMsg = e.Message as JoinMessage;
			if (joinMsg == null || String.Compare(joinMsg.Channel, Config.ChannelName, true) != 0)
				return;

			// ここに来るのは初回#Consoleを作成してJOINしたときのみ。
			// 二回目以降はサーバ側がJOINを送り出すのでこない。

			// IsSpecial を True にすることでチャンネルにタイムラインが流れないようにする
			CurrentSession.Groups[Config.ChannelName].IsSpecial = true;

			Console.ShowCommandsAsUsers();
		}

		/// <summary>
		/// フィルタ処理前のイベント
		/// </summary>
		private void CurrentSession_PreFilterProcessTimelineStatus(object sender, TimelineStatusEventArgs e)
		{
			ExceptionHandler(() =>
			{
				EnqueueInsertRequest(e.Status);
			});
		}

		/// <summary>
		/// Twitterのステータス更新を行った直後時のイベント
		/// </summary>
		private void CurrentSession_PostSendUpdateStatus(object sender, StatusUpdateEventArgs e)
		{
			ExceptionHandler(() =>
			{
				EnqueueInsertRequest(e.CreatedStatus);
			});
		}
		#endregion

		#region Execute
		/// <summary>
		/// コネクションを生成
		/// </summary>
		public void CreateConnection(Action<MySqlConnection> action)
		{
			CreateConnection<Int32>((con) =>
			{
				action(con);
				return 0;
			});
		}

		/// <summary>
		/// コネクションを生成
		/// </summary>
		public TResult CreateConnection<TResult>(Func<MySqlConnection, TResult> func)
		{
			using (MySqlConnection connection = new MySqlConnection(Config.ConnectionString))
			{
				connection.Open();
				return func(connection);
			}
		}

		/// <summary>
		/// トランザクションを生成
		/// </summary>
		public void CreateTransaction<TResult>(MySqlConnection con, Action<MySqlTransaction> action)
		{
			CreateTransaction<Int32>(con, (trans) =>
			{
				action(trans);
				return 0;
			});
		}

		/// <summary>
		/// トランザクションを生成
		/// </summary>
		public TResult CreateTransaction<TResult>(MySqlConnection con, Func<MySqlTransaction, TResult> func)
		{
			using (MySqlTransaction transaction = con.BeginTransaction())
			{
				try
				{
					TResult result = func(transaction);
					transaction.Commit();
					return result;
				}
				catch
				{
					transaction.Rollback();
					throw;
				}
			}
		}

		/// <summary>
		/// ExecuteReaderを実行
		/// </summary>
		public List<DBLoggerStatus> ExecuteReader(MySqlCommand command)
		{
			using (MySqlDataReader reader = command.ExecuteReader())
			{
				DataTable table = new DataTable();
				table.Load(reader);

				List<DBLoggerStatus> statuses = new List<DBLoggerStatus>();
				foreach (DataRow row in table.Rows)
				{
					DBLoggerStatus status = new DBLoggerStatus();
					status.StatusId = (Int64)row["status_id"];
					status.CreatedAt = (DateTime)row["created_at"];
					status.ScreenName = (String)row["screen_name"];
					status.Text = (String)row["text"] ?? String.Empty;
					statuses.Add(status);
				}

				return statuses;
			}
		}

		/// <summary>
		/// ExecuteScalarを実行
		/// </summary>
		public Object ExecuteScalar(MySqlCommand command)
		{
			return command.ExecuteScalar();
		}

		/// <summary>
		/// ExecuteNonQueryを実行する。
		/// </summary>
		public Int32 ExecuteNonQuery(MySqlCommand command)
		{
			return command.ExecuteNonQuery();
		}
		#endregion

		#region Simple
		/// <summary>
		/// Where句のみを指定して検索します。
		/// </summary>
		public List<DBLoggerStatus> Select(String where)
		{
			SelectLastWhere = where;
			SelectPage = 0;
			String query = MakeSelectQuery(SelectLastWhere, SelectPage);

			List<DBLoggerStatus> statuses = null;
			CreateConnection((con) =>
			{
				// 発行
				MySqlCommand command = new MySqlCommand(query, con);
				statuses = ExecuteReader(command);
			});

			return statuses;
		}

		/// <summary>
		/// 最後に実行したSelectで次のページを検索します。
		/// </summary>
		public List<DBLoggerStatus> SelectNextPage()
		{
			if (SelectLastWhere == String.Empty)
				throw new Exception("検索が行われていません。");

			String query = MakeSelectQuery(SelectLastWhere, ++SelectPage);

			List<DBLoggerStatus> statuses = null;
			CreateConnection((con) =>
			{
				// 発行
				MySqlCommand command = new MySqlCommand(query, con);
				statuses = ExecuteReader(command);
			});

			return statuses;
		}

		/// <summary>
		/// 最後に実行したSelectで前のページを検索します。
		/// </summary>
		public List<DBLoggerStatus> SelectPrevPage()
		{
			if (SelectLastWhere == String.Empty)
				throw new Exception("検索が行われていません。");
			if (SelectPage == 0)
				throw new Exception("前のページは存在しません。");

			String query = MakeSelectQuery(SelectLastWhere, --SelectPage);

			List<DBLoggerStatus> statuses = null;
			CreateConnection((con) =>
			{
				// 発行
				MySqlCommand command = new MySqlCommand(query, con);
				statuses = ExecuteReader(command);
			});

			return statuses;
		}

		/// <summary>
		/// DBLogger用のテーブルを作成する
		/// </summary>
		public void CreateDBLoggerTable()
		{
			String createTableQuery = String.Format(@"
				create table {0} (
					status_id bigint primary key,
					created_at datetime,
					screen_name varchar(32),
					text varchar(192),
					index(created_at, screen_name, text)
				) default charset utf8"
				, Config.TableName);

			CreateConnection((con) =>
			{
				MySqlCommand command = new MySqlCommand(createTableQuery, con);
				ExecuteNonQuery(command);
			});

			NotifyMessage("テーブルを作成しました。");
		}
		#endregion

		#region Insert
		/// <summary>
		/// ロギングを開始する。
		/// </summary>
		public void StartLogging()
		{
			if (_insertThread == null)
			{
				_insertThreadExitFlag = false;
				_insertThread = new Thread(StatusInsertionThread);
				_insertThreadEvent = new EventWaitHandle(false, EventResetMode.AutoReset);
				_insertThread.Start();
			}
		}

		/// <summary>
		/// ロギングを終了する。
		/// </summary>
		public void EndLogging()
		{
			if (_insertThread != null)
			{
				_insertThreadExitFlag = true;
				_insertThreadEvent.Set();
				_insertThread.Join();

				_insertThreadEvent.Close();
				_insertThreadEvent = null;
				_insertThread = null;
			}
		}

		/// <summary>
		/// Insertキューに追加する。
		/// </summary>
		public void EnqueueInsertRequest(Status status)
		{
			if (_insertThread != null)
			{
				lock (_insertQueue)
				{
					_insertQueue.Enqueue(DBLoggerHelper.StatusToDBLoggerStatus(status));
				}
			}
		}

		/// <summary>
		/// MySqlにInsertを投げる
		/// </summary>
		public void InsertStatuses(List<DBLoggerStatus> statuses)
		{
			if (statuses.Count > 0)
			{
				// 一気にINSERTして爆発すると怖いので100件ずつ
				const Int32 MAX_INSERTS = 100;
				Int32 startIndex = 0;

				while (startIndex < statuses.Count)
				{
					Int32 count = Math.Min(statuses.Count - startIndex, MAX_INSERTS);
					List<DBLoggerStatus> inserts = statuses.GetRange(startIndex, count);

					// SQL生成
					StringBuilder sb = new StringBuilder();
					sb.AppendFormat("insert ignore into {0} values", Config.TableName);
					ForEach(inserts, (s, i) =>
					{
						if (i != 0) sb.Append(",");
						sb.AppendFormat(" (?status_id{0}, ?created_at{0}, ?screen_name{0}, ?text{0})", i);
					});

					// パラメータをバインド
					MySqlCommand command = new MySqlCommand(sb.ToString());
					ForEach(inserts, (s, i) =>
					{
						command.Parameters.AddWithValue(String.Format("?status_id{0}", i), s.StatusId);
						command.Parameters.AddWithValue(String.Format("?created_at{0}", i), s.CreatedAt);
						command.Parameters.AddWithValue(String.Format("?screen_name{0}", i), s.ScreenName);
						command.Parameters.AddWithValue(String.Format("?text{0}", i), s.Text);
					});

					// 実行
					CreateConnection((con) =>
					{
						command.Connection = con;
						ExecuteNonQuery(command);
					});

					startIndex += MAX_INSERTS;
				}
			}
		}
		
		/// <summary>
		/// キューにたまったのをInsertするスレッド
		/// </summary>
		private void StatusInsertionThread()
		{
			while (true)
			{
				try
				{
					List<DBLoggerStatus> statuses = new List<DBLoggerStatus>();
					lock (_insertQueue)
					{
						// キューからあるだけ持ってく。
						statuses.AddRange(_insertQueue);
						_insertQueue.Clear();
					}

					// Insertする。
					InsertStatuses(statuses);

					// 終了フラグが立ってたらスレッドを終わらせる。
					if (_insertThreadExitFlag)
						break;

					// 適度に待機する。
					_insertThreadEvent.WaitOne(Config.IntervalInsert * 1000);
				}
				catch (MySqlException ex)
				{
					NotifyMessage(ex.Message);
					NotifyMessage(ex.StackTrace);

#if false
					// 致命的っぽいのでログ取りをやめる。
					_insertThreadEvent.Close();
					_insertThreadEvent = null;
					_insertThread = null;
					NotifyMessage("ロギングを中止しました。");
					break;
#else
					// とりあえず1分待つ
					Thread.Sleep(60 * 1000);
#endif
				}
				catch (Exception ex)
				{
					NotifyMessage(ex.Message);
				}
			}
		}
		#endregion
	}

	/// <summary>
	/// ヘルパ関数
	/// </summary>
	public static class DBLoggerHelper
	{
		/// <summary>
		/// 例外をハンドリングする
		/// </summary>
		public static void ExceptionHandler(Action action, Action<String> output)
		{
			try
			{
				action();
			}
			catch (MySqlException ex)
			{
				output(ex.Message);
				output(ex.StackTrace);
			}
			catch (Exception ex)
			{
				output(ex.Message);
				output(ex.StackTrace);
			}
		}

		/// <summary>
		/// コンソールをラップする
		/// </summary>
		public static Action<String> Notifier1(Misuzilla.Applications.TwitterIrcGateway.AddIns.Console.Console console)
		{
			return (message) => console.NotifyMessage(message);
		}

		/// <summary>
		/// コンソールをラップする
		/// </summary>
		public static Action<String, String> Notifier2(Misuzilla.Applications.TwitterIrcGateway.AddIns.Console.Console console)
		{
			return (nick, message) => console.NotifyMessage(nick, message);
		}

		/// <summary>
		/// 処理にかかった時間を取得する
		/// </summary>
		public static void ProcessTime(Action process, Action<String> time)
		{
			ProcessTimeWithPrefix(() =>
			{
				process();
				return String.Empty;
			}, time);
		}

		/// <summary>
		/// 処理にかかった時間をプリフィックスを取得する
		/// </summary>
		public static void ProcessTimeWithPrefix(Func<String> process, Action<String> time)
		{
			Stopwatch sw = Stopwatch.StartNew();
			String prefix = process();
			sw.Stop();

			StringBuilder sb = new StringBuilder();

			// プリフィックス
			if (!String.IsNullOrEmpty(prefix))
				sb.AppendFormat("{0} ", prefix);

			// 時間
			sb.AppendFormat("({0:F} 秒)", sw.ElapsedMilliseconds * 0.001f);

			// 処理
			time(sb.ToString());
		}

		/// <summary>
		/// DBLoggerのStatusからTIGのStatusに変換
		/// </summary>
		public static Status DBLoggerStatusToStatus(DBLoggerStatus status)
		{
			return new Status()
			{
				Id = status.StatusId,
				CreatedAt = status.CreatedAt,
				Text = status.Text ?? String.Empty,
				User = new User()
				{
					ScreenName = status.ScreenName,
				},
			};
		}

		/// <summary>
		/// TIGのStatusからDBLoggerのStatusに変換
		/// </summary>
		/// <param name="status"></param>
		/// <returns></returns>
		public static DBLoggerStatus StatusToDBLoggerStatus(Status status)
		{
			return new DBLoggerStatus()
			{
				StatusId = status.Id,
				CreatedAt = status.CreatedAt,
				Text = status.Text ?? String.Empty,
				ScreenName = status.User.ScreenName,
			};
		}

		/// <summary>
		/// ステータスを表示できる形式に処理する。
		/// </summary>
		public static void ProcessDBLoggerStatuses(Session session, DBLoggerAddIn dblogger, List<DBLoggerStatus> statuses, Action<String, String> action)
		{
			if (statuses.Count > 0)
			{
				statuses.Sort((s1, s2) => ((s1.CreatedAt == s2.CreatedAt) ? 0 : ((s1.CreatedAt > s2.CreatedAt) ? 1 : -1)));
				foreach (DBLoggerStatus status in statuses)
				{
					// データベースの型から標準のStatusに変換
					Status s = DBLoggerStatusToStatus(status);

					// メッセージ作成
					StringBuilder sb = new StringBuilder();
					sb.AppendFormat("{0}: {1}", s.CreatedAt.ToString(dblogger.Config.DateTimeFormat), s.Text);

					// TypableMap対応
					AppendTypableMap(session, dblogger, sb, s);

					// 処理
					action(s.User.ScreenName, sb.ToString());
				}
			}
		}

		/// <summary>
		/// TypableMapの情報を付与
		/// </summary>
		public static void AppendTypableMap(Session session, DBLoggerAddIn dblogger, StringBuilder stringBuilder, Status status)
		{
			if (session.Config.EnableTypableMap && dblogger.TypableMapCommand != null)
			{
				string typableMapId = dblogger.TypableMapCommand.TypableMap.Add(status);

				// TypableMapKeyColorNumber = -1 の場合には色がつかなくなる
				if (session.Config.TypableMapKeyColorNumber < 0)
					stringBuilder.AppendFormat(" ({0})", typableMapId);
				else
					stringBuilder.AppendFormat(" \x0003{0}({1})", session.Config.TypableMapKeyColorNumber, typableMapId);
			}
		}
	}

	/// <summary>
	/// クエリ実行ヘルパクラス
	/// </summary>
	public class DBLoggerQueryExecuter
	{
		public Session Session { get; set; }
		public DBLoggerAddIn AddIn { get; set; }
		public Misuzilla.Applications.TwitterIrcGateway.AddIns.Console.Console Console { get; set; }

		/// <summary>
		/// コンストラクタ
		/// </summary>
		public DBLoggerQueryExecuter(Session session, Misuzilla.Applications.TwitterIrcGateway.AddIns.Console.Console console)
		{
			Session = session;
			AddIn = session.AddInManager.GetAddIn<DBLoggerAddIn>();
			Console = console;
		}

		/// <summary>
		/// 例外ハンドラ
		/// </summary>
		public void ExceptionHandler(Action action)
		{
			DBLoggerHelper.ExceptionHandler(action, DBLoggerHelper.Notifier1(Console));
		}

		/// <summary>
		/// 処理にかかった時間を表示する
		/// </summary>
		public void ShowProcessTime(Action process)
		{
			DBLoggerHelper.ProcessTime(process, DBLoggerHelper.Notifier1(Console));
		}

		/// <summary>
		/// 処理にかかった時間をプリフィックスをつけて表示する
		/// </summary>
		public void ShowProcessTimeWithPrefix(Func<String> process)
		{
			DBLoggerHelper.ProcessTimeWithPrefix(process, DBLoggerHelper.Notifier1(Console));
		}

		/// <summary>
		/// ステータスを表示する。
		/// </summary>
		public void ShowStatuses(List<DBLoggerStatus> statuses)
		{
			if (statuses.Count == 0)
				Console.NotifyMessage("検索結果は見つかりませんでした。");
			else
				DBLoggerHelper.ProcessDBLoggerStatuses(Session, AddIn, statuses, DBLoggerHelper.Notifier2(Console));
		}

		/// <summary>
		/// 任意の処理を実行する
		/// </summary>
		public void Execute(Action action)
		{
			ShowProcessTime(() =>
			{
				action();
			});
		}

		/// <summary>
		/// 任意の処理を実行する
		/// </summary>
		public void Execute(Func<String> func)
		{
			ShowProcessTimeWithPrefix(() =>
			{
				return func();
			});
		}
	}
}
