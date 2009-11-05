using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ComponentModel;
using Misuzilla.Applications.TwitterIrcGateway;
using Misuzilla.Applications.TwitterIrcGateway.AddIns;
using Misuzilla.Applications.TwitterIrcGateway.AddIns.Console;
using Misuzilla.Applications.TwitterIrcGateway.AddIns.TypableMap;

namespace Spica.Applications.TwitterIrcGateway.AddIns.DBLogger
{
	public class UserDefinedQuery : IConfiguration
	{
		public String Query { get; set; }

		public UserDefinedQuery()
		{
			Query = String.Empty;
		}

		public override string ToString()
		{
			return String.Format("{0}", Query);
		}
	}

	public abstract class BaseUserDefinedQueryContext : Context
	{
		protected DBLoggerAddIn AddIn { get { return CurrentSession.AddInManager.GetAddIn<DBLoggerAddIn>(); } }
		protected List<UserDefinedQuery> UserDefinedQueryList
		{
			get { return AddIn.Config.UserDefinedQueryList; }
			set { AddIn.Config.UserDefinedQueryList = value; }
		}
	}

	public class UserDefinedQueryContext : BaseUserDefinedQueryContext
	{
		[Description("存在するクエリをすべて表示します")]
		public void List()
		{
			if (AddIn.Config.UserDefinedQueryList.Count == 0)
			{
				Console.NotifyMessage("クエリは現在設定されていません。");
				return;
			}

			for (Int32 i = 0; i < UserDefinedQueryList.Count; ++i)
			{
				UserDefinedQuery query = UserDefinedQueryList[i];
				Console.NotifyMessage(String.Format("{0}: {1}", i, query.ToString()));
			}
		}

		[Description("指定したクエリを削除します")]
		public void Remove(String arg)
		{
			FindAt(arg, item =>
			{
				UserDefinedQueryList.Remove(item);
				CurrentSession.AddInManager.SaveConfig(AddIn.Config);
				Console.NotifyMessage(String.Format("クエリ {0} を削除しました。", item.Query));
			});
		}

		[Description("指定したクエリを編集します")]
		public void Edit(String arg)
		{
			FindAt(arg, item =>
			{
				Type type = typeof(EditUserDefinedQueryContext);
				EditUserDefinedQueryContext ctx = Console.GetContext(type, CurrentServer, CurrentSession) as EditUserDefinedQueryContext;

				// TODO: Generic版が使えるようになったらちゃんとしたのに変更する。
				ctx.SetDefaultPattern(item);
				Console.PushContext(ctx);
			});
		}

		[Description("クエリを新規追加します")]
		public void New()
		{
			Type type = typeof(EditUserDefinedQueryContext);
			EditUserDefinedQueryContext ctx = Console.GetContext(type, CurrentServer, CurrentSession) as EditUserDefinedQueryContext;
			Console.PushContext(ctx);
		}

		#region Private
		private void FindAt(String arg, Action<UserDefinedQuery> action)
		{
			try
			{
				UserDefinedQuery query = UserDefinedQueryList[Int32.Parse(arg)];
				action(query);
			}
			catch (Exception)
			{
				Console.NotifyMessage("クエリの指定が正しくありません。");
			}
		}
		#endregion
	}

	public class EditUserDefinedQueryContext : BaseUserDefinedQueryContext
	{
		private Boolean IsNewRecord { get; set; }
		private UserDefinedQuery Query { get; set; }

		public override IConfiguration[] Configurations { get { return new IConfiguration[] { Query }; } }
		public override string ContextName { get { return (IsNewRecord ? "New" : "Edit") + typeof(UserDefinedQuery).Name; } }

		public EditUserDefinedQueryContext()
		{
			IsNewRecord = true;
			Query = new UserDefinedQuery();
		}

		[Browsable(false)]
		public void SetDefaultPattern(UserDefinedQuery query)
		{
			IsNewRecord = false;
			Query = query;
		}

		[Description("クエリを保存してコンテキストを終了します")]
		public void Save()
		{
			if (IsNewRecord) UserDefinedQueryList.Add(Query);
			CurrentSession.AddInManager.SaveConfig(AddIn.Config);
			Console.NotifyMessage(String.Format("クエルを{0}しました。", (IsNewRecord ? "新規作成" : "保存")));
			Exit();
		}
	}
}
