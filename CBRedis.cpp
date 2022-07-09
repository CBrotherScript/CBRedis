#include "ICBModule.h"

#include <string.h>
#include <stdio.h>

#include "hiredis.h"

#ifdef _WIN32
#define strcasecmp stricmp
#include <WinSock2.h>
#endif

#include <string>
using namespace std;

ICBObject* ReplyToCBObject(ICBrother* pCBrother,redisReply *reply,bool isbytes = false)
{
	ICBObject* resObj = NULL;
	switch (reply->type)
	{
	case REDIS_REPLY_STRING:
		{
			if (isbytes)
			{
				resObj = pCBrother->CreateCBClassObject("ByteArray");
				IClassObject* clsObj = resObj->ClassObjValue();
				ICBByteBuffer* byteArray = (ICBByteBuffer*)clsObj->GetUserParm();
				byteArray->WriteBytes(reply->str,reply->len);
			}
			else
			{
				resObj = pCBrother->CreateCBObject(reply->str,reply->len);
			}
			break;
		}
	case REDIS_REPLY_ARRAY:
		{
			resObj = pCBrother->CreateCBClassObject("Array");
			for (int i = 0 ; i < reply->elements ; i++)
			{
				ICBObject* childObj = ReplyToCBObject(pCBrother,reply->element[i]);
				pCBrother->CallCBClassFunc(resObj,"add",1,childObj);
				pCBrother->ReleaseCBObject(childObj);
			}
			break;
		}
	case REDIS_REPLY_INTEGER:
		{
			resObj = pCBrother->CreateCBObject(reply->integer);
			break;
		}
	case REDIS_REPLY_DOUBLE:
		{
			resObj = pCBrother->CreateCBObject((float)reply->dval);
			break;
		}
	case REDIS_REPLY_BOOL:
		{
			resObj = pCBrother->CreateCBObject((bool)reply->integer);
			break;
		}
	}

	return resObj;
}

ICBObject* CBRedis_Command_OK(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException,const char* cmd)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	redisReply *reply = (redisReply *)redisCommand(ctx, cmd);

	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_STATUS)
	{
		freeReplyObject(reply);
		return pCBrother->CreateCBObject(false);
	}

	freeReplyObject(reply);
	bool suc = strcasecmp(reply->str,"OK") == 0;
	freeReplyObject(reply);
	return pCBrother->CreateCBObject(suc);
}

ICBObject* CBRedis_Command_Key_Get_Value(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException,const char* cmd,bool isBytes)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	if (keyObj == NULL || keyObj->Type() != CB_STRING)
	{
		return NULL;
	}

	const char* key = keyObj->StringVal();

	redisReply *reply = (redisReply *)redisCommand(ctx, "%s %s",cmd,key);
	if (reply == NULL)
	{
		if (ctx->err)
		{
			pException = pCBrother->CreateException("RedisException", ctx->errstr);
		}
		return NULL;
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply,isBytes);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_Command_Key_Field_Get_Value(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException,const char* cmd,bool isBytes)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* fieldObj = args.GetCBObject(1);
	if (keyObj == NULL || keyObj->Type() != CB_STRING || fieldObj == NULL || fieldObj->Type() != CB_STRING)
	{
		return NULL;
	}

	const char* key = keyObj->StringVal();
	const char* field = fieldObj->StringVal();

	redisReply *reply = (redisReply *)redisCommand(ctx, "%s %s %s",cmd,key,field);
	if (reply == NULL)
	{
		if (ctx->err)
		{
			pException = pCBrother->CreateException("RedisException", ctx->errstr);
		}
		return NULL;
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply,isBytes);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_Command_OneKey_Interger(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException,const char* cmd)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	if (keyObj == NULL || keyObj->Type() != CB_STRING)
	{
		return pCBrother->CreateCBObject(0);
	}

	const char* key = keyObj->StringVal();
	redisReply *reply = (redisReply *)redisCommand(ctx, "%s %s",cmd, key);
	if (reply == NULL)
	{
		if (ctx->err)
		{
			pException = pCBrother->CreateException("RedisException", ctx->errstr);
		}
		return pCBrother->CreateCBObject(0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return pCBrother->CreateCBObject(0);
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_Command_KeyValue_Bool(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException,const char* cmd)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* valueObj = args.GetCBObject(1);
	if (keyObj == NULL || valueObj == NULL || keyObj->Type() != CB_STRING)
	{
		return pCBrother->CreateCBObject(false);
	}

	const char* key = keyObj->StringVal();
	redisReply *reply = NULL;

	switch (valueObj->Type())
	{
	case CB_INT:
		{
			reply = (redisReply *)redisCommand(ctx, "%s %s %d",cmd,key,valueObj->IntVal());
			break;
		}
	case CB_UINT:
		{
			reply = (redisReply *)redisCommand(ctx, "%s %s %u",cmd,key,(unsigned int)valueObj->IntVal());
			break;
		}
	case CB_LONG:
		{
			reply = (redisReply *)redisCommand(ctx, "%s %s %lld",cmd,key,valueObj->LongVal());
			break;
		}
	case CB_FLOAT:
		{
			reply = (redisReply *)redisCommand(ctx, "%s %s %f",cmd,key,valueObj->FloatVal());
			break;
		}
	case CB_BOOL:
		{
			reply = (redisReply *)redisCommand(ctx, "%s %s %s",cmd,key,valueObj->BoolVal() ? "true" : "false");
			break;
		}
	case CB_STRING:
		{
			int len = 0;
			const char* v = valueObj->BytesVal(len);
			reply = (redisReply *)redisCommand(ctx, "%s %s %b",cmd,key,v,(size_t)len);
			break;
		}
	case CB_CLASS:
		{
			ICBObject* lenObj = args.GetCBObject(2);
			IClassObject* clsObj = valueObj->ClassObjValue();
			if (strcasecmp(clsObj->GetCBClassName(),"ByteArray") != 0)
			{
				return pCBrother->CreateCBObject(false);
			}

			if (!pCBrother->WriteLockCBClsObject(clsObj))
			{
				pCBrother->CreateException("SyncException","multi thread access at object func! 'ByteArray' Object!");
				return pCBrother->CreateCBObject(false);
			}

			ICBByteBuffer* byteArray = (ICBByteBuffer*)clsObj->GetUserParm();
			const char* data = byteArray->GetBuf() + byteArray->GetReadPos();
			size_t len = byteArray->GetWritePos() - byteArray->GetReadPos();
			if (lenObj != NULL)
			{
				len = lenObj->AnyTypeToInt();
			}

			reply = (redisReply *)redisCommand(ctx, "%s %s %b",cmd,key,data,len);
			pCBrother->WriteUnlockCBClsObject(clsObj);
			break;
		}
	}

	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return pCBrother->CreateCBObject(false);
	}

	bool suc = reply->integer;
	freeReplyObject(reply);
	return pCBrother->CreateCBObject(suc);
}

ICBObject* CBRedis_Command_KeyValue_Value(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException,const char* cmd)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* valueObj = args.GetCBObject(1);
	if (keyObj == NULL || valueObj == NULL || keyObj->Type() != CB_STRING)
	{
		return pCBrother->CreateCBObject(false);
	}

	const char* key = keyObj->StringVal();
	redisReply *reply = NULL;

	switch (valueObj->Type())
	{
	case CB_INT:
		{
			reply = (redisReply *)redisCommand(ctx, "%s %s %d",cmd,key,valueObj->IntVal());
			break;
		}
	case CB_UINT:
		{
			reply = (redisReply *)redisCommand(ctx, "%s %s %u",cmd,key,(unsigned int)valueObj->IntVal());
			break;
		}
	case CB_LONG:
		{
			reply = (redisReply *)redisCommand(ctx, "%s %s %lld",cmd,key,valueObj->LongVal());
			break;
		}
	case CB_FLOAT:
		{
			reply = (redisReply *)redisCommand(ctx, "%s %s %f",cmd,key,valueObj->FloatVal());
			break;
		}
	case CB_BOOL:
		{
			reply = (redisReply *)redisCommand(ctx, "%s %s %s",cmd,key,valueObj->BoolVal() ? "true" : "false");
			break;
		}
	case CB_STRING:
		{
			int len = 0;
			const char* v = valueObj->BytesVal(len);
			reply = (redisReply *)redisCommand(ctx, "%s %s %b",cmd,key,v,(size_t)len);
			break;
		}
	case CB_CLASS:
		{
			ICBObject* lenObj = args.GetCBObject(2);
			IClassObject* clsObj = valueObj->ClassObjValue();
			if (strcasecmp(clsObj->GetCBClassName(),"ByteArray") != 0)
			{
				return pCBrother->CreateCBObject(false);
			}

			if (!pCBrother->WriteLockCBClsObject(clsObj))
			{
				pCBrother->CreateException("SyncException","multi thread access at object func! 'ByteArray' Object!");
				return pCBrother->CreateCBObject(false);
			}

			ICBByteBuffer* byteArray = (ICBByteBuffer*)clsObj->GetUserParm();
			const char* data = byteArray->GetBuf() + byteArray->GetReadPos();
			size_t len = byteArray->GetWritePos() - byteArray->GetReadPos();
			if (lenObj != NULL)
			{
				len = lenObj->AnyTypeToInt();
			}

			reply = (redisReply *)redisCommand(ctx, "%s %s %b",cmd,key,data,len);
			pCBrother->WriteUnlockCBClsObject(clsObj);
			break;
		}
	}

	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_Init(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return NULL;
}

void CBRedis_Release(ICBrother* pCBrother,IClassObject* obj,int releaseType)
{
	if (releaseType != CB_RELEASE)
	{
		return;
	}

	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx != NULL)
	{
		redisFree(ctx);
		obj->SetUserParm(NULL);
	}
}

ICBObject* CBRedis_Connect(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx != NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is already connected.");
		return NULL;
	}

	ICBObject* ipObj = args.GetCBObject(0);
	ICBObject* portObj = args.GetCBObject(1);
	ICBObject* timeoutObj = args.GetCBObject(2);

	if (ipObj == NULL || ipObj->Type() != CB_STRING || portObj == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	const char* ip = ipObj->StringVal();
	int port = portObj->AnyTypeToInt();

	if (timeoutObj == NULL)
	{
		ctx = redisConnect(ip, port);
	}
	else
	{
		unsigned int ms = timeoutObj->AnyTypeToInt();
		timeval to;
		to.tv_sec =  ms / 1000;
		to.tv_usec = (ms - to.tv_sec * 1000) * 100;
		ctx = redisConnectWithTimeout(ip, port,to);
	}

	if (ctx == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (ctx->err)
	{
		char buf[1024] = {0};
		sprintf(buf,"redis connect err! %s",ctx->errstr);
		pException = pCBrother->CreateException("RedisException",buf);
		redisFree(ctx);
		return NULL;
	}

	obj->SetUserParm(ctx);
	return pCBrother->CreateCBObject(true);
}

ICBObject* CBRedis_Close(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	redisFree(ctx);
	obj->SetUserParm(ctx);
	return pCBrother->CreateCBObject(true);
}

ICBObject* CBRedis_Auth(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* pwdObj = args.GetCBObject(0);
	if (pwdObj == NULL || pwdObj->Type() != CB_STRING)
	{
		return pCBrother->CreateCBObject(false);
	}

	const char* pwd = pwdObj->StringVal();
	redisReply *reply = (redisReply *)redisCommand(ctx, "AUTH %s", pwd);
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_STATUS)
	{
		freeReplyObject(reply);
		return pCBrother->CreateCBObject(false);
	}

	bool suc = strcasecmp(reply->str,"OK") == 0;
	freeReplyObject(reply);
	return pCBrother->CreateCBObject(suc);
}

ICBObject* CBRedis_Select(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* dbObj = args.GetCBObject(0);
	if (dbObj == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	int db = dbObj->AnyTypeToInt();
	redisReply *reply = (redisReply *)redisCommand(ctx, "SELECT %d", db);
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_STATUS)
	{
		freeReplyObject(reply);
		return pCBrother->CreateCBObject(false);
	}

	bool suc = strcasecmp(reply->str,"OK") == 0;
	freeReplyObject(reply);
	return pCBrother->CreateCBObject(suc);
}

ICBObject* CBRedis_Ping(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	redisReply *reply = (redisReply *)redisCommand(ctx, "PING");
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_STATUS)
	{
		freeReplyObject(reply);
		return pCBrother->CreateCBObject(false);
	}

	bool suc = strcasecmp(reply->str,"PONG") == 0;
	freeReplyObject(reply);
	return pCBrother->CreateCBObject(suc);
}

ICBObject* CBRedis_Incr(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_OneKey_Interger(pCBrother,args,obj,pException,"INCR");
}

ICBObject* CBRedis_Decr(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_OneKey_Interger(pCBrother,args,obj,pException,"DECR");
}

ICBObject* CBRedis_Append(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_KeyValue_Value(pCBrother,args,obj,pException,"APPEND");
}

ICBObject* CBRedis_IncrBy(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_KeyValue_Value(pCBrother,args,obj,pException,"INCRBY");
}

ICBObject* CBRedis_IncrByFloat(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_KeyValue_Value(pCBrother,args,obj,pException,"INCRBYFLOAT");
}

ICBObject* CBRedis_DecrBy(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_KeyValue_Value(pCBrother,args,obj,pException,"DECRBY");
}

ICBObject* CBRedis_Exists(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	if (keyObj == NULL || keyObj->Type() != CB_STRING)
	{
		return pCBrother->CreateCBObject(false);
	}

	const char* key = keyObj->StringVal();
	redisReply *reply = (redisReply *)redisCommand(ctx, "EXISTS %s",key);
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return pCBrother->CreateCBObject(false);
	}

	bool suc = reply->integer;
	freeReplyObject(reply);
	return pCBrother->CreateCBObject(suc);
}

ICBObject* CBRedis_Delete(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	if (keyObj == NULL || keyObj->Type() != CB_STRING)
	{
		return pCBrother->CreateCBObject(false);
	}

	const char* key = keyObj->StringVal();

	redisReply *reply = (redisReply *)redisCommand(ctx, "DEL %s",key);
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_STATUS)
	{
		freeReplyObject(reply);
		return pCBrother->CreateCBObject(false);
	}

	bool suc = strcasecmp(reply->str,"OK") == 0;
	freeReplyObject(reply);
	return pCBrother->CreateCBObject(suc);
}

ICBObject* CBRedis_Expire(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* timeObj = args.GetCBObject(1);
	if (keyObj == NULL || keyObj->Type() != CB_STRING || timeObj == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	const char* key = keyObj->StringVal();
	unsigned int t = timeObj->AnyTypeToInt();

	redisReply *reply = (redisReply *)redisCommand(ctx, "EXPIRE %s %u",key,t);
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return pCBrother->CreateCBObject(false);
	}

	bool suc = reply->integer;
	freeReplyObject(reply);
	return pCBrother->CreateCBObject(suc);
}

ICBObject* CBRedis_PExpire(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* timeObj = args.GetCBObject(1);
	if (keyObj == NULL || keyObj->Type() != CB_STRING || timeObj == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	const char* key = keyObj->StringVal();
	unsigned int t = timeObj->AnyTypeToInt();

	redisReply *reply = (redisReply *)redisCommand(ctx, "PEXPIRE %s %u",key,t);
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return pCBrother->CreateCBObject(false);
	}

	bool suc = reply->integer;
	freeReplyObject(reply);
	return pCBrother->CreateCBObject(suc);
}

ICBObject* CBRedis_Persist(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	if (keyObj == NULL || keyObj->Type() != CB_STRING)
	{
		return pCBrother->CreateCBObject(false);
	}

	const char* key = keyObj->StringVal();
	redisReply *reply = (redisReply *)redisCommand(ctx, "PERSIST %s",key);
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return pCBrother->CreateCBObject(false);
	}

	bool suc = reply->integer;
	freeReplyObject(reply);
	return pCBrother->CreateCBObject(suc);
}

ICBObject* CBRedis_Move(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* timeObj = args.GetCBObject(1);
	if (keyObj == NULL || keyObj->Type() != CB_STRING || timeObj == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	const char* key = keyObj->StringVal();
	unsigned int t = timeObj->AnyTypeToInt();

	redisReply *reply = (redisReply *)redisCommand(ctx, "MOVE %s %u",key,t);
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return pCBrother->CreateCBObject(false);
	}

	bool suc = reply->integer;
	freeReplyObject(reply);
	return pCBrother->CreateCBObject(suc);
}

ICBObject* CBRedis_TTL(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_OneKey_Interger(pCBrother,args,obj,pException,"TTL");
}

ICBObject* CBRedis_PTTL(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_OneKey_Interger(pCBrother,args,obj,pException,"PTTL");
}

ICBObject* CBRedis_Strlen(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_OneKey_Interger(pCBrother,args,obj,pException,"STRLEN");
}

ICBObject* CBRedis_Type(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	if (keyObj == NULL || keyObj->Type() != CB_STRING)
	{
		return NULL;
	}

	const char* key = keyObj->StringVal();

	redisReply *reply = (redisReply *)redisCommand(ctx, "TYPE %s",key);
	if (reply == NULL)
	{
		return NULL;
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_STATUS)
	{
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = pCBrother->CreateCBObject(reply->str,reply->len);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_Set(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* valueObj = args.GetCBObject(1);
	if (keyObj == NULL || valueObj == NULL || keyObj->Type() != CB_STRING)
	{
		return pCBrother->CreateCBObject(false);
	}

	const char* key = keyObj->StringVal();
	redisReply *reply = NULL;

	switch (valueObj->Type())
	{
	case CB_INT:
		{
			reply = (redisReply *)redisCommand(ctx, "SET %s %d",key,valueObj->IntVal());
			break;
		}
	case CB_UINT:
		{
			reply = (redisReply *)redisCommand(ctx, "SET %s %u",key,(unsigned int)valueObj->IntVal());
			break;
		}
	case CB_LONG:
		{
			reply = (redisReply *)redisCommand(ctx, "SET %s %lld",key,valueObj->LongVal());
			break;
		}
	case CB_FLOAT:
		{
			reply = (redisReply *)redisCommand(ctx, "SET %s %f",key,valueObj->FloatVal());
			break;
		}
	case CB_BOOL:
		{
			reply = (redisReply *)redisCommand(ctx, "SET %s %s",key,valueObj->BoolVal() ? "true" : "false");
			break;
		}
	case CB_STRING:
		{
			int len = 0;
			const char* v = valueObj->BytesVal(len);
			reply = (redisReply *)redisCommand(ctx, "SET %s %b",key,v,(size_t)len);
			break;
		}
	case CB_CLASS:
		{
			ICBObject* lenObj = args.GetCBObject(2);
			IClassObject* clsObj = valueObj->ClassObjValue();
			if (strcasecmp(clsObj->GetCBClassName(),"ByteArray") != 0)
			{
				return pCBrother->CreateCBObject(false);
			}

			if (!pCBrother->WriteLockCBClsObject(clsObj))
			{
				pCBrother->CreateException("SyncException","multi thread access at object func! 'ByteArray' Object!");
				return pCBrother->CreateCBObject(false);
			}

			ICBByteBuffer* byteArray = (ICBByteBuffer*)clsObj->GetUserParm();
			const char* data = byteArray->GetBuf() + byteArray->GetReadPos();
			size_t len = byteArray->GetWritePos() - byteArray->GetReadPos();
			if (lenObj != NULL)
			{
				len = lenObj->AnyTypeToInt();
			}

			reply = (redisReply *)redisCommand(ctx, "SET %s %b",key,data,len);			
			pCBrother->WriteUnlockCBClsObject(clsObj);
			break;
		}
	}

	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_STATUS)
	{
		freeReplyObject(reply);
		return pCBrother->CreateCBObject(false);
	}

	bool suc = strcasecmp(reply->str,"OK") == 0;
	freeReplyObject(reply);
	return pCBrother->CreateCBObject(suc);
}

ICBObject* CBRedis_Get(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_Key_Get_Value(pCBrother,args,obj,pException,"GET",false);
}

ICBObject* CBRedis_GetBytes(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_Key_Get_Value(pCBrother,args,obj,pException,"GET",true);
}

ICBObject* CBRedis_HSet(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* fieldObj = args.GetCBObject(1);
	ICBObject* valueObj = args.GetCBObject(2);
	if (keyObj == NULL || valueObj == NULL || fieldObj == NULL|| keyObj->Type() != CB_STRING || fieldObj->Type() != CB_STRING)
	{
		return pCBrother->CreateCBObject(false);
	}

	const char* key = keyObj->StringVal();
	const char* field = fieldObj->StringVal();
	redisReply *reply = NULL;

	switch (valueObj->Type())
	{
	case CB_INT:
		{
			reply = (redisReply *)redisCommand(ctx, "HSET %s %s %d",key,field,valueObj->IntVal());
			break;
		}
	case CB_UINT:
		{
			reply = (redisReply *)redisCommand(ctx, "HSET %s %s %u",key,field,(unsigned int)valueObj->IntVal());
			break;
		}
	case CB_LONG:
		{
			reply = (redisReply *)redisCommand(ctx, "HSET %s %s %lld",key,field,valueObj->LongVal());
			break;
		}
	case CB_FLOAT:
		{
			reply = (redisReply *)redisCommand(ctx, "HSET %s %s %f",key,field,valueObj->FloatVal());
			break;
		}
	case CB_BOOL:
		{
			reply = (redisReply *)redisCommand(ctx, "HSET %s %s %s",key,field,valueObj->BoolVal() ? "true" : "false");
			break;
		}
	case CB_STRING:
		{
			int len = 0;
			const char* v = valueObj->BytesVal(len);
			reply = (redisReply *)redisCommand(ctx, "HSET %s %s %b",key,field,v,(size_t)len);
			break;
		}
	case CB_CLASS:
		{
			ICBObject* lenObj = args.GetCBObject(2);
			IClassObject* clsObj = valueObj->ClassObjValue();
			if (strcasecmp(clsObj->GetCBClassName(),"ByteArray") != 0)
			{
				return pCBrother->CreateCBObject(false);
			}

			if (!pCBrother->WriteLockCBClsObject(clsObj))
			{
				pCBrother->CreateException("SyncException","multi thread access at object func! 'ByteArray' Object!");
				return pCBrother->CreateCBObject(false);
			}

			ICBByteBuffer* byteArray = (ICBByteBuffer*)clsObj->GetUserParm();
			const char* data = byteArray->GetBuf() + byteArray->GetReadPos();
			size_t len = byteArray->GetWritePos() - byteArray->GetReadPos();
			if (lenObj != NULL)
			{
				len = lenObj->AnyTypeToInt();
			}

			reply = (redisReply *)redisCommand(ctx, "HSET %s %s %b",key,field,data,len);			
			pCBrother->WriteUnlockCBClsObject(clsObj);
			break;
		}
	}

	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_STATUS)
	{
		freeReplyObject(reply);
		return pCBrother->CreateCBObject(false);
	}

	bool suc = strcasecmp(reply->str,"OK") == 0;
	freeReplyObject(reply);
	return pCBrother->CreateCBObject(suc);
}

ICBObject* CBRedis_HGet(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* fieldObj = args.GetCBObject(1);
	if (keyObj == NULL || keyObj->Type() != CB_STRING || fieldObj == NULL || fieldObj->Type() != CB_STRING)
	{
		return pCBrother->CreateCBObject(false);
	}

	const char* key = keyObj->StringVal();
	const char* field = fieldObj->StringVal();

	redisReply *reply = (redisReply *)redisCommand(ctx, "HGET %s %s",key,field);
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_HGetBytes(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* fieldObj = args.GetCBObject(1);
	if (keyObj == NULL || keyObj->Type() != CB_STRING || fieldObj == NULL || fieldObj->Type() != CB_STRING)
	{
		return pCBrother->CreateCBObject(false);
	}

	const char* key = keyObj->StringVal();
	const char* field = fieldObj->StringVal();

	redisReply *reply = (redisReply *)redisCommand(ctx, "HGET %s %s",key,field);
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply,true);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_HExists(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* fieldObj = args.GetCBObject(1);
	if (keyObj == NULL || keyObj->Type() != CB_STRING || fieldObj == NULL || fieldObj->Type() != CB_STRING)
	{
		return pCBrother->CreateCBObject(false);
	}

	const char* key = keyObj->StringVal();
	const char* field = fieldObj->StringVal();

	redisReply *reply = (redisReply *)redisCommand(ctx, "HEXISTS %s %s",key,field);
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return pCBrother->CreateCBObject(false);
	}

	ICBObject* resObj = pCBrother->CreateCBObject(reply->integer == 1);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_HLen(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_OneKey_Interger(pCBrother,args,obj,pException,"HLEN");
}

ICBObject* CBRedis_HKeys(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	if (keyObj == NULL || keyObj->Type() != CB_STRING)
	{
		return pCBrother->CreateCBClassObject("Array");
	}

	const char* key = keyObj->StringVal();

	redisReply *reply = (redisReply *)redisCommand(ctx, "HKEYS %s",key);
	if (reply == NULL)
	{
		return pCBrother->CreateCBClassObject("Array");
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_HVals(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	if (keyObj == NULL || keyObj->Type() != CB_STRING)
	{
		return pCBrother->CreateCBClassObject("Array");
	}

	const char* key = keyObj->StringVal();

	redisReply *reply = (redisReply *)redisCommand(ctx, "HVALS %s",key);
	if (reply == NULL)
	{
		return pCBrother->CreateCBClassObject("Array");
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_HGetAll(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	if (keyObj == NULL || keyObj->Type() != CB_STRING)
	{
		return pCBrother->CreateCBObject(0);
	}

	const char* key = keyObj->StringVal();

	redisReply *reply = (redisReply *)redisCommand(ctx, "HGETALL %s",key);
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_HDel(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	if (keyObj == NULL || keyObj->Type() != CB_STRING)
	{
		return pCBrother->CreateCBObject(0);
	}

	const char* key = keyObj->StringVal();

	string fieldStr = "HDEL ";
	fieldStr += key;
	for (int i = 1 ; i < args.Size() ; i++)
	{
		ICBObject* fieldObj = args.GetCBObject(i);
		if (fieldObj == NULL || fieldObj->Type() != CB_STRING)
		{
			return pCBrother->CreateCBObject(0);
		}
		fieldStr += " ";
		fieldStr += fieldObj->StringVal();
	}

	redisReply *reply = (redisReply *)redisCommand(ctx, fieldStr.c_str());
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_LLen(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_OneKey_Interger(pCBrother,args,obj,pException,"LLEN");
}

ICBObject* CBRedis_LRange(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* beginObj = args.GetCBObject(1);
	ICBObject* endObj = args.GetCBObject(2);
	if (keyObj == NULL || keyObj->Type() != CB_STRING || beginObj == NULL || endObj == NULL)
	{
		return pCBrother->CreateCBObject(0);
	}

	const char* key = keyObj->StringVal();
	int beginPos = beginObj->AnyTypeToInt();
	int endPos = endObj->AnyTypeToInt();
	redisReply *reply = (redisReply *)redisCommand(ctx, "LRANGE %s %d %d", key,beginPos,endPos);
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_LRpush(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException,const char* cmd)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* valueObj = args.GetCBObject(1);
	if (keyObj == NULL || keyObj->Type() != CB_STRING || valueObj == NULL)
	{
		return pCBrother->CreateCBObject(-1);
	}

	const char* key = keyObj->StringVal();
	redisReply *reply = NULL;
	switch (valueObj->Type())
	{
	case CB_INT:
	case CB_UINT:
	case CB_LONG:
	case CB_FLOAT:
	case CB_BOOL:
		{
			char buf[64] = {0};
			reply = (redisReply *)redisCommand(ctx, "%s %s %s",cmd,key,valueObj->AnyTypeToString(buf));
			break;
		}
	case CB_STRING:
		{
			int len = 0;
			const char* v = valueObj->BytesVal(len);
			reply = (redisReply *)redisCommand(ctx, "%s %s %b",cmd,key,v,(size_t)len);
			break;
		}
	case CB_CLASS:
		{
			ICBObject* lenObj = args.GetCBObject(3);
			IClassObject* clsObj = valueObj->ClassObjValue();
			if (strcasecmp(clsObj->GetCBClassName(),"ByteArray") != 0)
			{
				return pCBrother->CreateCBObject(-1);
			}

			if (!pCBrother->WriteLockCBClsObject(clsObj))
			{
				pCBrother->CreateException("SyncException","multi thread access at object func! 'ByteArray' Object!");
				return NULL;
			}

			ICBByteBuffer* byteArray = (ICBByteBuffer*)clsObj->GetUserParm();
			const char* data = byteArray->GetBuf() + byteArray->GetReadPos();
			size_t len = byteArray->GetWritePos() - byteArray->GetReadPos();
			if (lenObj != NULL)
			{
				len = lenObj->AnyTypeToInt();
			}

			reply = (redisReply *)redisCommand(ctx, "%s %s %b",cmd,key,data,len);
			pCBrother->WriteUnlockCBClsObject(clsObj);
			break;
		}
	}

	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(-1);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_RPush(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_LRpush(pCBrother,args,obj,pException,"RPUSH");
}

ICBObject* CBRedis_LPush(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_LRpush(pCBrother,args,obj,pException,"LPUSH");
}

ICBObject* CBRedis_LPop(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	if (keyObj == NULL || keyObj->Type() != CB_STRING)
	{
		return pCBrother->CreateCBObject(0);
	}

	const char* key = keyObj->StringVal();
	redisReply *reply = (redisReply *)redisCommand(ctx, "LPOP %s", key);
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_RPop(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	if (keyObj == NULL || keyObj->Type() != CB_STRING)
	{
		return pCBrother->CreateCBObject(0);
	}

	const char* key = keyObj->StringVal();
	redisReply *reply = (redisReply *)redisCommand(ctx, "RPOP %s", key);
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_LRem(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* countObj = args.GetCBObject(1);
	ICBObject* valueObj = args.GetCBObject(2);
	if (keyObj == NULL || keyObj->Type() != CB_STRING || valueObj == NULL || countObj == NULL)
	{
		return pCBrother->CreateCBObject(0);
	}

	const char* key = keyObj->StringVal();
	int count = countObj->AnyTypeToInt();
	redisReply *reply = NULL;
	switch (valueObj->Type())
	{
	case CB_INT:
	case CB_UINT:
	case CB_LONG:
	case CB_FLOAT:
	case CB_BOOL:
		{
			char buf[64] = {0};
			reply = (redisReply *)redisCommand(ctx, "LREM %s %d %s",key,count,valueObj->AnyTypeToString(buf));
			break;
		}
	case CB_STRING:
		{
			int len = 0;
			const char* v = valueObj->BytesVal(len);
			reply = (redisReply *)redisCommand(ctx, "LREM %s %d %b",key,count,v,(size_t)len);
			break;
		}
	case CB_CLASS:
		{
			ICBObject* lenObj = args.GetCBObject(3);
			IClassObject* clsObj = valueObj->ClassObjValue();
			if (strcasecmp(clsObj->GetCBClassName(),"ByteArray") != 0)
			{
				return pCBrother->CreateCBObject(-1);
			}

			if (!pCBrother->WriteLockCBClsObject(clsObj))
			{
				pCBrother->CreateException("SyncException","multi thread access at object func! 'ByteArray' Object!");
				return NULL;
			}

			ICBByteBuffer* byteArray = (ICBByteBuffer*)clsObj->GetUserParm();
			const char* data = byteArray->GetBuf() + byteArray->GetReadPos();
			size_t len = byteArray->GetWritePos() - byteArray->GetReadPos();
			if (lenObj != NULL)
			{
				len = lenObj->AnyTypeToInt();
			}

			reply = (redisReply *)redisCommand(ctx, "LREM %s %d %b",key,count,data,len);
			pCBrother->WriteUnlockCBClsObject(clsObj);
			break;
		}
	}

	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_LIndex(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* posObj = args.GetCBObject(1);
	if (keyObj == NULL || keyObj->Type() != CB_STRING || posObj == NULL)
	{
		return pCBrother->CreateCBObject(0);
	}

	const char* key = keyObj->StringVal();
	int pos = posObj->AnyTypeToInt();
	redisReply *reply = (redisReply *)redisCommand(ctx, "LINDEX %s %d", key,pos);
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_LInsert(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException,bool isBefore)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* value1Obj = args.GetCBObject(1);
	ICBObject* value2Obj = args.GetCBObject(2);
	if (keyObj == NULL || keyObj->Type() != CB_STRING || value1Obj == NULL || value2Obj == NULL)
	{
		return pCBrother->CreateCBObject(-1);
	}

	const char* key = keyObj->StringVal();
	redisReply *reply = NULL;

	switch (value1Obj->Type())
	{
	case CB_INT:
	case CB_UINT:
	case CB_LONG:
	case CB_FLOAT:
	case CB_BOOL:
		{
			char buf[64] = {0};
			const char* value1 = value1Obj->AnyTypeToString(buf);
			switch (value2Obj->Type())
			{
			case CB_INT:
			case CB_UINT:
			case CB_LONG:
			case CB_FLOAT:
			case CB_BOOL:
				{
					char buf2[64] = {0};
					const char* value2 = value2Obj->AnyTypeToString(buf2);
					if (isBefore)
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s BEFORE %s %s",key,value1,value2);
					}
					else
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s AFTER %s %s",key,value1,value2);
					}					
					break;
				}
			case CB_STRING:
				{
					int len = 0;
					const char* v = value2Obj->BytesVal(len);
					if (isBefore)
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s BEFORE %s %b",key,value1,v,(size_t)len);
					}
					else
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s AFTER %s %b",key,value1,v,(size_t)len);
					}
					break;
				}
			case CB_CLASS:
				{
					ICBObject* lenObj = args.GetCBObject(3);
					IClassObject* clsObj = value2Obj->ClassObjValue();
					if (strcasecmp(clsObj->GetCBClassName(),"ByteArray") != 0)
					{
						return pCBrother->CreateCBObject(-1);
					}

					if (!pCBrother->WriteLockCBClsObject(clsObj))
					{
						pCBrother->CreateException("SyncException","multi thread access at object func! 'ByteArray' Object!");
						return NULL;
					}

					ICBByteBuffer* byteArray = (ICBByteBuffer*)clsObj->GetUserParm();
					const char* data = byteArray->GetBuf() + byteArray->GetReadPos();
					size_t len = byteArray->GetWritePos() - byteArray->GetReadPos();
					if (lenObj != NULL)
					{
						len = lenObj->AnyTypeToInt();
					}

					if (isBefore)
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s BEFORE %s %b",key,value1,data,len);
					}
					else
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s AFTER %s %b",key,value1,data,len);
					}
					pCBrother->WriteUnlockCBClsObject(clsObj);
					break;
				}
			}			
			
			break;
		}
	case CB_STRING:
		{
			int len1 = 0;
			const char* value1 = value1Obj->BytesVal(len1);
			switch (value2Obj->Type())
			{
			case CB_INT:
			case CB_UINT:
			case CB_LONG:
			case CB_FLOAT:
			case CB_BOOL:
				{
					char buf2[64] = {0};
					const char* value2 = value2Obj->AnyTypeToString(buf2);
					if (isBefore)
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s BEFORE %b %s",key,value1,(size_t)len1,value2);
					}
					else
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s AFTER %b %s",key,value1,(size_t)len1,value2);
					}
					break;
				}
			case CB_STRING:
				{
					int len = 0;
					const char* v = value2Obj->BytesVal(len);
					if (isBefore)
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s BEFORE %b %b",key,value1,(size_t)len1,v,(size_t)len);
					}
					else
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s AFTER %b %b",key,value1,(size_t)len1,v,(size_t)len);
					}
					break;
				}
			case CB_CLASS:
				{
					ICBObject* lenObj = args.GetCBObject(3);
					IClassObject* clsObj = value2Obj->ClassObjValue();
					if (strcasecmp(clsObj->GetCBClassName(),"ByteArray") != 0)
					{
						return pCBrother->CreateCBObject(-1);
					}

					if (!pCBrother->WriteLockCBClsObject(clsObj))
					{
						pCBrother->CreateException("SyncException","multi thread access at object func! 'ByteArray' Object!");
						return NULL;
					}

					ICBByteBuffer* byteArray = (ICBByteBuffer*)clsObj->GetUserParm();
					const char* data = byteArray->GetBuf() + byteArray->GetReadPos();
					size_t len = byteArray->GetWritePos() - byteArray->GetReadPos();
					if (lenObj != NULL)
					{
						len = lenObj->AnyTypeToInt();
					}

					if (isBefore)
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s BEFORE %b %b",key,value1,(size_t)len1,data,len);
					}
					else
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s AFTER %b %b",key,value1,(size_t)len1,data,len);
					}
					pCBrother->WriteUnlockCBClsObject(clsObj);
					break;
				}
			}
			break;
		}
	case CB_CLASS:
		{
			IClassObject* clsObj1 = value1Obj->ClassObjValue();
			if (strcasecmp(clsObj1->GetCBClassName(),"ByteArray") != 0)
			{
				return pCBrother->CreateCBObject(-1);
			}

			if (!pCBrother->WriteLockCBClsObject(clsObj1))
			{
				pCBrother->CreateException("SyncException","multi thread access at object func! 'ByteArray' Object!");
				return NULL;
			}

			ICBByteBuffer* byteArray1 = (ICBByteBuffer*)clsObj1->GetUserParm();
			const char* value1 = byteArray1->GetBuf() + byteArray1->GetReadPos();
			size_t len1 = byteArray1->GetWritePos() - byteArray1->GetReadPos();

			switch (value2Obj->Type())
			{
			case CB_INT:
			case CB_UINT:
			case CB_LONG:
			case CB_FLOAT:
			case CB_BOOL:
				{
					char buf2[64] = {0};
					const char* value2 = value2Obj->AnyTypeToString(buf2);
					if (isBefore)
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s BEFORE %b %s",key,value1,len1,value2);
					}
					else
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s AFTER %b %s",key,value1,len1,value2);
					}
					break;
				}
			case CB_STRING:
				{
					int len = 0;
					const char* v = value2Obj->BytesVal(len);
					if (isBefore)
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s BEFORE %b %b",key,value1,len1,v,(size_t)len);
					}
					else
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s AFTER %b %b",key,value1,len1,v,(size_t)len);
					}
					break;
				}
			case CB_CLASS:
				{
					ICBObject* lenObj = args.GetCBObject(3);
					IClassObject* clsObj = value2Obj->ClassObjValue();
					if (strcasecmp(clsObj->GetCBClassName(),"ByteArray") != 0)
					{
						pCBrother->WriteUnlockCBClsObject(clsObj1);
						return pCBrother->CreateCBObject(-1);
					}

					if (!pCBrother->WriteLockCBClsObject(clsObj))
					{
						pCBrother->WriteUnlockCBClsObject(clsObj1);
						pCBrother->CreateException("SyncException","multi thread access at object func! 'ByteArray' Object!");
						return NULL;
					}

					ICBByteBuffer* byteArray = (ICBByteBuffer*)clsObj->GetUserParm();
					const char* data = byteArray->GetBuf() + byteArray->GetReadPos();
					size_t len = byteArray->GetWritePos() - byteArray->GetReadPos();
					if (lenObj != NULL)
					{
						len = lenObj->AnyTypeToInt();
					}

					if (isBefore)
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s BEFORE %b %b",key,value1,len1,data,len);
					}
					else
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s AFTER %b %b",key,value1,len1,data,len);
					}
					pCBrother->WriteUnlockCBClsObject(clsObj);
					break;
				}
			}

			pCBrother->WriteUnlockCBClsObject(clsObj1);
			break;
		}
	}

	if (reply == NULL)
	{
		if (ctx->err != 0)
		{
			pException = pCBrother->CreateException("RedisException", ctx->errstr);
		}
		return NULL;
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_LInsertBefore(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_LInsert(pCBrother,args,obj,pException,true);
}

ICBObject* CBRedis_LInsertAfter(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_LInsert(pCBrother,args,obj,pException,false);
}

ICBObject* CBRedis_LSet(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* countObj = args.GetCBObject(1);
	ICBObject* valueObj = args.GetCBObject(2);
	if (keyObj == NULL || keyObj->Type() != CB_STRING || valueObj == NULL || countObj == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	const char* key = keyObj->StringVal();
	int count = countObj->AnyTypeToInt();
	redisReply *reply = NULL;
	switch (valueObj->Type())
	{
	case CB_INT:
	case CB_UINT:
	case CB_LONG:
	case CB_FLOAT:
	case CB_BOOL:
		{
			char buf[64] = {0};
			reply = (redisReply *)redisCommand(ctx, "LSET %s %d %s",key,count,valueObj->AnyTypeToString(buf));
			break;
		}
	case CB_STRING:
		{
			int len = 0;
			const char* v = valueObj->BytesVal(len);
			reply = (redisReply *)redisCommand(ctx, "LSET %s %d %b",key,count,v,(size_t)len);
			break;
		}
	case CB_CLASS:
		{
			ICBObject* lenObj = args.GetCBObject(3);
			IClassObject* clsObj = valueObj->ClassObjValue();
			if (strcasecmp(clsObj->GetCBClassName(),"ByteArray") != 0)
			{
				return pCBrother->CreateCBObject(-1);
			}

			if (!pCBrother->WriteLockCBClsObject(clsObj))
			{
				pCBrother->CreateException("SyncException","multi thread access at object func! 'ByteArray' Object!");
				return NULL;
			}

			ICBByteBuffer* byteArray = (ICBByteBuffer*)clsObj->GetUserParm();
			const char* data = byteArray->GetBuf() + byteArray->GetReadPos();
			size_t len = byteArray->GetWritePos() - byteArray->GetReadPos();
			if (lenObj != NULL)
			{
				len = lenObj->AnyTypeToInt();
			}

			reply = (redisReply *)redisCommand(ctx, "LSET %s %d %b",key,count,data,len);
			pCBrother->WriteUnlockCBClsObject(clsObj);
			break;
		}
	}

	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_STATUS)
	{
		freeReplyObject(reply);
		return pCBrother->CreateCBObject(false);
	}

	bool suc = strcasecmp(reply->str,"OK") == 0;
	freeReplyObject(reply);
	return pCBrother->CreateCBObject(suc);
}

ICBObject* CBRedis_Command_Integer(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException,const char* cmd)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	redisReply *reply = (redisReply *)redisCommand(ctx, cmd);

	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return pCBrother->CreateCBObject(0);
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_DbSize(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_Integer(pCBrother,args,obj,pException,"DBSIZE");
}

ICBObject* CBRedis_LastSave(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_Integer(pCBrother,args,obj,pException,"LASTSAVE");
}

ICBObject* CBRedis_BgSave(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	redisReply *reply = (redisReply *)redisCommand(ctx, "BGSAVE");

	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_STATUS)
	{
		freeReplyObject(reply);
		return pCBrother->CreateCBObject(false);
	}

	freeReplyObject(reply);
	bool suc = strcasecmp(reply->str,"Background saving started") == 0;
	freeReplyObject(reply);
	return pCBrother->CreateCBObject(suc);
}

ICBObject* CBRedis_Save(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_OK(pCBrother,args,obj,pException,"SAVE");
}

ICBObject* CBRedis_FlushAll(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_OK(pCBrother,args,obj,pException,"FLUSHALL");
}

ICBObject* CBRedis_FlushDB(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_OK(pCBrother,args,obj,pException,"FLUSHDB");
}

ICBObject* CBRedis_SAdd(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_KeyValue_Bool(pCBrother,args,obj,pException,"SADD");
}

ICBObject* CBRedis_SCard(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_OneKey_Interger(pCBrother,args,obj,pException,"SCARD");
}

ICBObject* CBRedis_SRem(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_KeyValue_Bool(pCBrother,args,obj,pException,"SREM");
}

ICBObject* CBRedis_SMembers(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_Key_Get_Value(pCBrother,args,obj,pException,"SMEMBERS",false);
}

ICBObject* CBRedis_Command_Key_Cnt_Value(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException,const char* cmd)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* cntObj = args.GetCBObject(1);
	if (keyObj == NULL || keyObj->Type() != CB_STRING)
	{
		return pCBrother->CreateCBObject(0);
	}

	const char* key = keyObj->StringVal();
	int cnt = -1;
	if (cntObj != NULL)
	{
		cnt = cntObj->AnyTypeToInt();
	}

	redisReply *reply = NULL;
	if (cnt == -1)
	{
		reply = (redisReply *)redisCommand(ctx, "%s %s",cmd, key);
	}
	else
	{
		reply = (redisReply *)redisCommand(ctx, "%s %s %d",cmd, key,cnt);
	}

	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_SPop(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_Key_Cnt_Value(pCBrother,args,obj,pException,"SPOP");
}

ICBObject* CBRedis_SRandmember(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_Key_Cnt_Value(pCBrother,args,obj,pException,"SRANDMEMBER");
}

ICBObject* CBRedis_SInter(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_Key_Field_Get_Value(pCBrother,args,obj,pException,"SINTER",false);
}

ICBObject* CBRedis_SUnion(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_Key_Field_Get_Value(pCBrother,args,obj,pException,"SUNION",false);
}

ICBObject* CBRedis_SDiff(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_Key_Field_Get_Value(pCBrother,args,obj,pException,"SDIFF",false);
}

ICBObject* CBRedis_ZAdd(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* scoreObj = args.GetCBObject(1);
	ICBObject* valueObj = args.GetCBObject(2);
	if (keyObj == NULL || keyObj->Type() != CB_STRING || scoreObj == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	const char* key = keyObj->StringVal();
	int score = scoreObj->AnyTypeToInt();
	redisReply *reply = NULL;
	switch (valueObj->Type())
	{
	case CB_INT:
	case CB_UINT:
	case CB_LONG:
	case CB_FLOAT:
	case CB_BOOL:
		{
			char buf[64] = {0};
			reply = (redisReply *)redisCommand(ctx, "ZADD %s %d %s",key,score,valueObj->AnyTypeToString(buf));
			break;
		}
	case CB_STRING:
		{
			int len = 0;
			const char* v = valueObj->BytesVal(len);
			reply = (redisReply *)redisCommand(ctx, "ZADD %s %d %b",key,score,v,(size_t)len);
			break;
		}
	case CB_CLASS:
		{
			ICBObject* lenObj = args.GetCBObject(3);
			IClassObject* clsObj = valueObj->ClassObjValue();
			if (strcasecmp(clsObj->GetCBClassName(),"ByteArray") != 0)
			{
				return pCBrother->CreateCBObject(false);
			}

			if (!pCBrother->WriteLockCBClsObject(clsObj))
			{
				pCBrother->CreateException("SyncException","multi thread access at object func! 'ByteArray' Object!");
				return NULL;
			}

			ICBByteBuffer* byteArray = (ICBByteBuffer*)clsObj->GetUserParm();
			const char* data = byteArray->GetBuf() + byteArray->GetReadPos();
			size_t len = byteArray->GetWritePos() - byteArray->GetReadPos();
			if (lenObj != NULL)
			{
				len = lenObj->AnyTypeToInt();
			}

			reply = (redisReply *)redisCommand(ctx, "ZADD %s %d %b",key,score,data,len);
			pCBrother->WriteUnlockCBClsObject(clsObj);
			break;
		}
	}

	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return pCBrother->CreateCBObject(false);
	}

	bool suc = reply->integer;
	freeReplyObject(reply);
	return pCBrother->CreateCBObject(suc);
}

ICBObject* CBRedis_ZCard(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_OneKey_Interger(pCBrother,args,obj,pException,"ZCARD");
}

ICBObject* CBRedis_ZCount(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* beginObj = args.GetCBObject(1);
	ICBObject* endObj = args.GetCBObject(2);
	if (keyObj == NULL || keyObj->Type() != CB_STRING || beginObj == NULL || endObj == NULL)
	{
		return pCBrother->CreateCBObject(0);
	}

	const char* key = keyObj->StringVal();
	int beginPos = beginObj->AnyTypeToInt();
	int endPos = endObj->AnyTypeToInt();
	redisReply *reply = (redisReply *)redisCommand(ctx, "ZCOUNT %s %d %d", key,beginPos,endPos);
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return pCBrother->CreateCBObject(0);
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_ZRange(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* beginObj = args.GetCBObject(1);
	ICBObject* endObj = args.GetCBObject(2);
	if (keyObj == NULL || keyObj->Type() != CB_STRING || beginObj == NULL || endObj == NULL)
	{
		return pCBrother->CreateCBObject(0);
	}

	const char* key = keyObj->StringVal();
	int beginPos = beginObj->AnyTypeToInt();
	int endPos = endObj->AnyTypeToInt();
	redisReply *reply = (redisReply *)redisCommand(ctx, "ZRANGE %s %d %d WITHSCORES", key,beginPos,endPos);
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_ZRevrange(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	redisContext* ctx = (redisContext*)obj->GetUserParm();
	if (ctx == NULL)
	{
		pException = pCBrother->CreateException("RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = args.GetCBObject(0);
	ICBObject* beginObj = args.GetCBObject(1);
	ICBObject* endObj = args.GetCBObject(2);
	if (keyObj == NULL || keyObj->Type() != CB_STRING || beginObj == NULL || endObj == NULL)
	{
		return pCBrother->CreateCBObject(0);
	}

	const char* key = keyObj->StringVal();
	int beginPos = beginObj->AnyTypeToInt();
	int endPos = endObj->AnyTypeToInt();
	redisReply *reply = (redisReply *)redisCommand(ctx, "ZREVRANGE %s %d %d WITHSCORES", key,beginPos,endPos);
	if (reply == NULL)
	{
		return pCBrother->CreateCBObject(0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		pException = pCBrother->CreateException("RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_ZScore(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_Key_Field_Get_Value(pCBrother,args,obj,pException,"ZSCORE",false);
}

ICBObject* CBRedis_ZRank(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_Key_Field_Get_Value(pCBrother,args,obj,pException,"ZRANK",false);
}

ICBObject* CBRedis_ZRevrank(ICBrother* pCBrother,ICBObjectList &args,IClassObject* obj,ICBException* &pException)
{
	return CBRedis_Command_Key_Field_Get_Value(pCBrother,args,obj,pException,"ZREVRANK",false);
}

bool Init(ICBrother* pCBrother)
{
	//regiest your function and class
	pCBrother->RegisterCBClass("Client",CBRedis_Init,CBRedis_Release,"redis");

	pCBrother->RegisterCBClassFunc("redis::Client","connect",CBRedis_Connect,true);
	pCBrother->RegisterCBClassFunc("redis::Client","close",CBRedis_Close,true);
	pCBrother->RegisterCBClassFunc("redis::Client","auth",CBRedis_Auth,true);
	pCBrother->RegisterCBClassFunc("redis::Client","select",CBRedis_Select,true);
	pCBrother->RegisterCBClassFunc("redis::Client","ping",CBRedis_Ping,true);
	pCBrother->RegisterCBClassFunc("redis::Client","dbSize",CBRedis_DbSize,true);
	pCBrother->RegisterCBClassFunc("redis::Client","bgSave",CBRedis_BgSave,true);
	pCBrother->RegisterCBClassFunc("redis::Client","save",CBRedis_Save,true);
	pCBrother->RegisterCBClassFunc("redis::Client","flushAll",CBRedis_FlushAll,true);
	pCBrother->RegisterCBClassFunc("redis::Client","flushDB",CBRedis_FlushDB,true);
	pCBrother->RegisterCBClassFunc("redis::Client","lastSave",CBRedis_LastSave,true);

	pCBrother->RegisterCBClassFunc("redis::Client","exists",CBRedis_Exists,true);
	pCBrother->RegisterCBClassFunc("redis::Client","delete",CBRedis_Delete,true);
	pCBrother->RegisterCBClassFunc("redis::Client","expire",CBRedis_Expire,true);
	pCBrother->RegisterCBClassFunc("redis::Client","pexpire",CBRedis_PExpire,true);	
	pCBrother->RegisterCBClassFunc("redis::Client","persist",CBRedis_Persist,true);
	pCBrother->RegisterCBClassFunc("redis::Client","move",CBRedis_Move,true);	
	pCBrother->RegisterCBClassFunc("redis::Client","ttl",CBRedis_TTL,true);
	pCBrother->RegisterCBClassFunc("redis::Client","pttl",CBRedis_PTTL,true);
	pCBrother->RegisterCBClassFunc("redis::Client","strlen",CBRedis_Strlen,true);
	pCBrother->RegisterCBClassFunc("redis::Client","type",CBRedis_Type,true);
	pCBrother->RegisterCBClassFunc("redis::Client","set",CBRedis_Set,true);
	pCBrother->RegisterCBClassFunc("redis::Client","get",CBRedis_Get,true);
	pCBrother->RegisterCBClassFunc("redis::Client","getBytes",CBRedis_GetBytes,true);
	pCBrother->RegisterCBClassFunc("redis::Client","incr",CBRedis_Incr,true);
	pCBrother->RegisterCBClassFunc("redis::Client","incrBy",CBRedis_IncrBy,true);
	pCBrother->RegisterCBClassFunc("redis::Client","incrByFloat",CBRedis_IncrByFloat,true);	
	pCBrother->RegisterCBClassFunc("redis::Client","decr",CBRedis_Decr,true);
	pCBrother->RegisterCBClassFunc("redis::Client","decrBy",CBRedis_DecrBy,true);
	pCBrother->RegisterCBClassFunc("redis::Client","append",CBRedis_Append,true);	

	pCBrother->RegisterCBClassFunc("redis::Client","hset",CBRedis_HSet,true);
	pCBrother->RegisterCBClassFunc("redis::Client","hget",CBRedis_HGet,true);
	pCBrother->RegisterCBClassFunc("redis::Client","hgetBytes",CBRedis_HGetBytes,true);
	pCBrother->RegisterCBClassFunc("redis::Client","hexists",CBRedis_HExists,true);
	pCBrother->RegisterCBClassFunc("redis::Client","hlen",CBRedis_HLen,true);
	pCBrother->RegisterCBClassFunc("redis::Client","hkeys",CBRedis_HKeys,true);
	pCBrother->RegisterCBClassFunc("redis::Client","hvals",CBRedis_HVals,true);
	pCBrother->RegisterCBClassFunc("redis::Client","hgetAll",CBRedis_HGetAll,true);
	pCBrother->RegisterCBClassFunc("redis::Client","hdel",CBRedis_HDel,true);

	pCBrother->RegisterCBClassFunc("redis::Client","llen",CBRedis_LLen,true);
	pCBrother->RegisterCBClassFunc("redis::Client","lrange",CBRedis_LRange,true);
	pCBrother->RegisterCBClassFunc("redis::Client","rpush",CBRedis_RPush,true);
	pCBrother->RegisterCBClassFunc("redis::Client","lpush",CBRedis_LPush,true);
	pCBrother->RegisterCBClassFunc("redis::Client","lindex",CBRedis_LIndex,true);
	pCBrother->RegisterCBClassFunc("redis::Client","linsertAfter",CBRedis_LInsertAfter,true);
	pCBrother->RegisterCBClassFunc("redis::Client","linsertBefore",CBRedis_LInsertBefore,true);
	pCBrother->RegisterCBClassFunc("redis::Client","lpop",CBRedis_LPop,true);
	pCBrother->RegisterCBClassFunc("redis::Client","rpop",CBRedis_RPop,true);
	pCBrother->RegisterCBClassFunc("redis::Client","lrem",CBRedis_LRem,true);
	pCBrother->RegisterCBClassFunc("redis::Client","lset",CBRedis_LSet,true);
	
	pCBrother->RegisterCBClassFunc("redis::Client","sadd",CBRedis_SAdd,true);
	pCBrother->RegisterCBClassFunc("redis::Client","scard",CBRedis_SCard,true);
	pCBrother->RegisterCBClassFunc("redis::Client","srem",CBRedis_SRem,true);
	pCBrother->RegisterCBClassFunc("redis::Client","smembers",CBRedis_SMembers,true);
	pCBrother->RegisterCBClassFunc("redis::Client","spop",CBRedis_SPop,true);
	pCBrother->RegisterCBClassFunc("redis::Client","srandMember",CBRedis_SRandmember,true);
	pCBrother->RegisterCBClassFunc("redis::Client","sinter",CBRedis_SInter,true);
	pCBrother->RegisterCBClassFunc("redis::Client","sunion",CBRedis_SUnion,true);
	pCBrother->RegisterCBClassFunc("redis::Client","sdiff",CBRedis_SDiff,true);

	pCBrother->RegisterCBClassFunc("redis::Client","zadd",CBRedis_ZAdd,true);
	pCBrother->RegisterCBClassFunc("redis::Client","zcard",CBRedis_ZCard,true);
	pCBrother->RegisterCBClassFunc("redis::Client","zcount",CBRedis_ZCount,true);
	pCBrother->RegisterCBClassFunc("redis::Client","zrange",CBRedis_ZRange,true);
	pCBrother->RegisterCBClassFunc("redis::Client","zrevrange",CBRedis_ZRevrange,true);
	pCBrother->RegisterCBClassFunc("redis::Client","zscore",CBRedis_ZScore,true);
	pCBrother->RegisterCBClassFunc("redis::Client","zrank",CBRedis_ZRank,true);
	pCBrother->RegisterCBClassFunc("redis::Client","zrevrank",CBRedis_ZRevrank,true);	
	
	return true;
}
CREATE_CBROTHER_MODULE(Init)
