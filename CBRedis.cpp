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
				resObj = ICBrother_CreateCBClassObject(pCBrother,"ByteArray",NULL,0);
				IClassObject* clsObj = ICBObject_ClassObjValue(resObj);
				ICBByteArray* byteArray = (ICBByteArray*)IClassObject_GetUserParm(clsObj);
				ICBByteArray_WriteBytes(byteArray,reply->str,reply->len);
			}
			else
			{
				resObj = ICBrother_CreateCBObjectBytes(pCBrother,reply->str,reply->len);
			}
			break;
		}
	case REDIS_REPLY_ARRAY:
		{
			resObj = ICBrother_CreateCBClassObject(pCBrother,"Array",NULL,0);
			for (int i = 0 ; i < reply->elements ; i++)
			{
				ICBObject* childObj = ReplyToCBObject(pCBrother,reply->element[i]);
				ICBrother_CallCBClassFunc(pCBrother,resObj,"add",1,&childObj);
				ICBrother_ReleaseCBObject(pCBrother,childObj);
			}
			break;
		}
	case REDIS_REPLY_INTEGER:
		{
			resObj = ICBrother_CreateCBObjectInt64(pCBrother,reply->integer);
			break;
		}
	case REDIS_REPLY_DOUBLE:
		{
			resObj = ICBrother_CreateCBObjectFloat(pCBrother,(float)reply->dval);
			break;
		}
	case REDIS_REPLY_BOOL:
		{
			resObj = ICBrother_CreateCBObjectBool(pCBrother,(bool)reply->integer);
			break;
		}
	}

	return resObj;
}

ICBObject* CBRedis_Command_OK(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException,const char* cmd)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	redisReply *reply = (redisReply *)redisCommand(ctx, cmd);

	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_STATUS)
	{
		freeReplyObject(reply);
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	freeReplyObject(reply);
	bool suc = strcasecmp(reply->str,"OK") == 0;
	freeReplyObject(reply);
	return ICBrother_CreateCBObjectBool(pCBrother,suc);
}

ICBObject* CBRedis_Command_Key_Get_Value(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException,const char* cmd,bool isBytes)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING)
	{
		return NULL;
	}

	const char* key = ICBObject_StringVal(keyObj);

	redisReply *reply = (redisReply *)redisCommand(ctx, "%s %s",cmd,key);
	if (reply == NULL)
	{
		if (ctx->err)
		{
			*pException = ICBrother_CreateException(pCBrother,"RedisException", ctx->errstr);
		}
		return NULL;
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply,isBytes);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_Command_Key_Field_Get_Value(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException,const char* cmd,bool isBytes)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* fieldObj = ICBObjectList_GetCBObject(args,1);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING || fieldObj == NULL || ICBObject_Type(fieldObj) != CB_STRING)
	{
		return NULL;
	}

	const char* key = ICBObject_StringVal(keyObj);
	const char* field = ICBObject_StringVal(fieldObj);

	redisReply *reply = (redisReply *)redisCommand(ctx, "%s %s %s",cmd,key,field);
	if (reply == NULL)
	{
		if (ctx->err)
		{
			*pException = ICBrother_CreateException(pCBrother,"RedisException", ctx->errstr);
		}
		return NULL;
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply,isBytes);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_Command_OneKey_Interger(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException,const char* cmd)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	const char* key = ICBObject_StringVal(keyObj);
	redisReply *reply = (redisReply *)redisCommand(ctx, "%s %s",cmd, key);
	if (reply == NULL)
	{
		if (ctx->err)
		{
			*pException = ICBrother_CreateException(pCBrother,"RedisException", ctx->errstr);
		}
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_Command_KeyValue_Bool(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException,const char* cmd)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* valueObj = ICBObjectList_GetCBObject(args,1);
	if (keyObj == NULL || valueObj == NULL || ICBObject_Type(keyObj) != CB_STRING)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	const char* key = ICBObject_StringVal(keyObj);
	redisReply *reply = NULL;

	switch (ICBObject_Type(valueObj))
	{
	case CB_INT:
		{
			reply = (redisReply *)redisCommand(ctx, "%s %s %d",cmd,key,ICBObject_IntVal(valueObj));
			break;
		}
	case CB_UINT:
		{
			reply = (redisReply *)redisCommand(ctx, "%s %s %u",cmd,key,(unsigned int)ICBObject_IntVal(valueObj));
			break;
		}
	case CB_LONG:
		{
			reply = (redisReply *)redisCommand(ctx, "%s %s %lld",cmd,key,ICBObject_LongVal(valueObj));
			break;
		}
	case CB_FLOAT:
		{
			reply = (redisReply *)redisCommand(ctx, "%s %s %f",cmd,key,ICBObject_FloatVal(valueObj));
			break;
		}
	case CB_BOOL:
		{
			reply = (redisReply *)redisCommand(ctx, "%s %s %s",cmd,key,ICBObject_BoolVal(valueObj) ? "true" : "false");
			break;
		}
	case CB_STRING:
		{
			int len = 0;
			const char* v = ICBObject_BytesVal(valueObj,&len);
			reply = (redisReply *)redisCommand(ctx, "%s %s %b",cmd,key,v,(size_t)len);
			break;
		}
	case CB_CLASS:
		{
			ICBObject* lenObj = ICBObjectList_GetCBObject(args,2);
			IClassObject* clsObj = ICBObject_ClassObjValue(valueObj);
			if (strcasecmp(IClassObject_GetCBClassName(clsObj),"ByteArray") != 0)
			{
				return ICBrother_CreateCBObjectBool(pCBrother,false);
			}

			if (!ICBrother_WriteLockCBClsObject(pCBrother,clsObj))
			{
				ICBrother_CreateException(pCBrother,"SyncException","multi thread access at object func! 'ByteArray' Object!");
				return ICBrother_CreateCBObjectBool(pCBrother,false);
			}

			ICBByteArray* byteArray = (ICBByteArray*)IClassObject_GetUserParm(clsObj);
			const char* data = ICBByteArray_GetBuf(byteArray) + ICBByteArray_GetReadPos(byteArray);
			size_t len = ICBByteArray_GetWritePos(byteArray) - ICBByteArray_GetReadPos(byteArray);
			if (lenObj != NULL)
			{
				len = ICBObject_AnyTypeToInt(lenObj);
			}

			reply = (redisReply *)redisCommand(ctx, "%s %s %b",cmd,key,data,len);
			ICBrother_WriteUnlockCBClsObject(pCBrother,clsObj);
			break;
		}
	}

	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	bool suc = reply->integer;
	freeReplyObject(reply);
	return ICBrother_CreateCBObjectBool(pCBrother,suc);
}

ICBObject* CBRedis_Command_KeyValue_Value(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException,const char* cmd)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* valueObj = ICBObjectList_GetCBObject(args,1);
	if (keyObj == NULL || valueObj == NULL || ICBObject_Type(keyObj) != CB_STRING)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	const char* key = ICBObject_StringVal(keyObj);
	redisReply *reply = NULL;

	switch (ICBObject_Type(valueObj))
	{
	case CB_INT:
		{
			reply = (redisReply *)redisCommand(ctx, "%s %s %d",cmd,key,ICBObject_IntVal(valueObj));
			break;
		}
	case CB_UINT:
		{
			reply = (redisReply *)redisCommand(ctx, "%s %s %u",cmd,key,(unsigned int)ICBObject_IntVal(valueObj));
			break;
		}
	case CB_LONG:
		{
			reply = (redisReply *)redisCommand(ctx, "%s %s %lld",cmd,key,ICBObject_LongVal(valueObj));
			break;
		}
	case CB_FLOAT:
		{
			reply = (redisReply *)redisCommand(ctx, "%s %s %f",cmd,key,ICBObject_FloatVal(valueObj));
			break;
		}
	case CB_BOOL:
		{
			reply = (redisReply *)redisCommand(ctx, "%s %s %s",cmd,key,ICBObject_BoolVal(valueObj) ? "true" : "false");
			break;
		}
	case CB_STRING:
		{
			int len = 0;
			const char* v = ICBObject_BytesVal(valueObj,&len);
			reply = (redisReply *)redisCommand(ctx, "%s %s %b",cmd,key,v,(size_t)len);
			break;
		}
	case CB_CLASS:
		{
			ICBObject* lenObj = ICBObjectList_GetCBObject(args,2);
			IClassObject* clsObj = ICBObject_ClassObjValue(valueObj);
			if (strcasecmp(IClassObject_GetCBClassName(clsObj),"ByteArray") != 0)
			{
				return ICBrother_CreateCBObjectBool(pCBrother,false);
			}

			if (!ICBrother_WriteLockCBClsObject(pCBrother,clsObj))
			{
				ICBrother_CreateException(pCBrother,"SyncException","multi thread access at object func! 'ByteArray' Object!");
				return ICBrother_CreateCBObjectBool(pCBrother,false);
			}

			ICBByteArray* byteArray = (ICBByteArray*)IClassObject_GetUserParm(clsObj);
			const char* data = ICBByteArray_GetBuf(byteArray) + ICBByteArray_GetReadPos(byteArray);
			size_t len = ICBByteArray_GetWritePos(byteArray) - ICBByteArray_GetReadPos(byteArray);
			if (lenObj != NULL)
			{
				len = ICBObject_AnyTypeToInt(lenObj);
			}

			reply = (redisReply *)redisCommand(ctx, "%s %s %b",cmd,key,data,len);
			ICBrother_WriteUnlockCBClsObject(pCBrother,clsObj);
			break;
		}
	}

	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_Init(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return NULL;
}

void CBRedis_Release(ICBrother* pCBrother,IClassObject* obj,int releaseType)
{
	if (releaseType != CB_RELEASE)
	{
		return;
	}

	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx != NULL)
	{
		redisFree(ctx);
		IClassObject_SetUserParm(obj,NULL);
	}
}

ICBObject* CBRedis_Connect(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx != NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is already connected.");
		return NULL;
	}

	ICBObject* ipObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* portObj = ICBObjectList_GetCBObject(args,1);
	ICBObject* timeoutObj = ICBObjectList_GetCBObject(args,2);

	if (ipObj == NULL || ICBObject_Type(ipObj) != CB_STRING || portObj == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	const char* ip = ICBObject_StringVal(ipObj);
	int port = ICBObject_AnyTypeToInt(portObj);

	if (timeoutObj == NULL)
	{
		ctx = redisConnect(ip, port);
	}
	else
	{
		unsigned int ms = ICBObject_AnyTypeToInt(timeoutObj);
		timeval to;
		to.tv_sec =  ms / 1000;
		to.tv_usec = (ms - to.tv_sec * 1000) * 100;
		ctx = redisConnectWithTimeout(ip, port,to);
	}

	if (ctx == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (ctx->err)
	{
		char buf[1024] = {0};
		sprintf(buf,"redis connect err! %s",ctx->errstr);
		*pException = ICBrother_CreateException(pCBrother,"RedisException",buf);
		redisFree(ctx);
		return NULL;
	}

	IClassObject_SetUserParm(obj,ctx);
	return ICBrother_CreateCBObjectBool(pCBrother,true);
}

ICBObject* CBRedis_Close(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	redisFree(ctx);
	IClassObject_SetUserParm(obj,ctx);
	return ICBrother_CreateCBObjectBool(pCBrother,true);
}

ICBObject* CBRedis_Auth(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* pwdObj = ICBObjectList_GetCBObject(args,0);
	if (pwdObj == NULL || ICBObject_Type(pwdObj) != CB_STRING)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	const char* pwd = ICBObject_StringVal(pwdObj);
	redisReply *reply = (redisReply *)redisCommand(ctx, "AUTH %s", pwd);
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_STATUS)
	{
		freeReplyObject(reply);
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	bool suc = strcasecmp(reply->str,"OK") == 0;
	freeReplyObject(reply);
	return ICBrother_CreateCBObjectBool(pCBrother,suc);
}

ICBObject* CBRedis_Select(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* dbObj = ICBObjectList_GetCBObject(args,0);
	if (dbObj == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	int db = ICBObject_AnyTypeToInt(dbObj);
	redisReply *reply = (redisReply *)redisCommand(ctx, "SELECT %d", db);
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_STATUS)
	{
		freeReplyObject(reply);
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	bool suc = strcasecmp(reply->str,"OK") == 0;
	freeReplyObject(reply);
	return ICBrother_CreateCBObjectBool(pCBrother,suc);
}

ICBObject* CBRedis_Ping(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	redisReply *reply = (redisReply *)redisCommand(ctx, "PING");
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_STATUS)
	{
		freeReplyObject(reply);
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	bool suc = strcasecmp(reply->str,"PONG") == 0;
	freeReplyObject(reply);
	return ICBrother_CreateCBObjectBool(pCBrother,suc);
}

ICBObject* CBRedis_Incr(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_OneKey_Interger(pCBrother,args,obj,pException,"INCR");
}

ICBObject* CBRedis_Decr(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_OneKey_Interger(pCBrother,args,obj,pException,"DECR");
}

ICBObject* CBRedis_Append(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_KeyValue_Value(pCBrother,args,obj,pException,"APPEND");
}

ICBObject* CBRedis_IncrBy(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_KeyValue_Value(pCBrother,args,obj,pException,"INCRBY");
}

ICBObject* CBRedis_IncrByFloat(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_KeyValue_Value(pCBrother,args,obj,pException,"INCRBYFLOAT");
}

ICBObject* CBRedis_DecrBy(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_KeyValue_Value(pCBrother,args,obj,pException,"DECRBY");
}

ICBObject* CBRedis_Exists(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	const char* key = ICBObject_StringVal(keyObj);
	redisReply *reply = (redisReply *)redisCommand(ctx, "EXISTS %s",key);
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	bool suc = reply->integer;
	freeReplyObject(reply);
	return ICBrother_CreateCBObjectBool(pCBrother,suc);
}

ICBObject* CBRedis_Delete(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	const char* key = ICBObject_StringVal(keyObj);

	redisReply *reply = (redisReply *)redisCommand(ctx, "DEL %s",key);
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_STATUS)
	{
		freeReplyObject(reply);
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	bool suc = strcasecmp(reply->str,"OK") == 0;
	freeReplyObject(reply);
	return ICBrother_CreateCBObjectBool(pCBrother,suc);
}

ICBObject* CBRedis_Expire(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* timeObj = ICBObjectList_GetCBObject(args,1);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING || timeObj == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	const char* key = ICBObject_StringVal(keyObj);
	unsigned int t = ICBObject_AnyTypeToInt(timeObj);

	redisReply *reply = (redisReply *)redisCommand(ctx, "EXPIRE %s %u",key,t);
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	bool suc = reply->integer;
	freeReplyObject(reply);
	return ICBrother_CreateCBObjectBool(pCBrother,suc);
}

ICBObject* CBRedis_PExpire(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* timeObj = ICBObjectList_GetCBObject(args,1);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING || timeObj == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	const char* key = ICBObject_StringVal(keyObj);
	unsigned int t = ICBObject_AnyTypeToInt(timeObj);

	redisReply *reply = (redisReply *)redisCommand(ctx, "PEXPIRE %s %u",key,t);
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	bool suc = reply->integer;
	freeReplyObject(reply);
	return ICBrother_CreateCBObjectBool(pCBrother,suc);
}

ICBObject* CBRedis_Persist(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	const char* key = ICBObject_StringVal(keyObj);
	redisReply *reply = (redisReply *)redisCommand(ctx, "PERSIST %s",key);
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	bool suc = reply->integer;
	freeReplyObject(reply);
	return ICBrother_CreateCBObjectBool(pCBrother,suc);
}

ICBObject* CBRedis_Move(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* timeObj = ICBObjectList_GetCBObject(args,1);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING || timeObj == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	const char* key = ICBObject_StringVal(keyObj);
	unsigned int t = ICBObject_AnyTypeToInt(timeObj);

	redisReply *reply = (redisReply *)redisCommand(ctx, "MOVE %s %u",key,t);
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	bool suc = reply->integer;
	freeReplyObject(reply);
	return ICBrother_CreateCBObjectBool(pCBrother,suc);
}

ICBObject* CBRedis_TTL(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_OneKey_Interger(pCBrother,args,obj,pException,"TTL");
}

ICBObject* CBRedis_PTTL(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_OneKey_Interger(pCBrother,args,obj,pException,"PTTL");
}

ICBObject* CBRedis_Strlen(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_OneKey_Interger(pCBrother,args,obj,pException,"STRLEN");
}

ICBObject* CBRedis_Type(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING)
	{
		return NULL;
	}

	const char* key = ICBObject_StringVal(keyObj);

	redisReply *reply = (redisReply *)redisCommand(ctx, "TYPE %s",key);
	if (reply == NULL)
	{
		return NULL;
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_STATUS)
	{
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ICBrother_CreateCBObjectBytes(pCBrother,reply->str,reply->len);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_Set(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* valueObj = ICBObjectList_GetCBObject(args,1);
	if (keyObj == NULL || valueObj == NULL || ICBObject_Type(keyObj) != CB_STRING)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	const char* key = ICBObject_StringVal(keyObj);
	redisReply *reply = NULL;

	switch (ICBObject_Type(valueObj))
	{
	case CB_INT:
		{
			reply = (redisReply *)redisCommand(ctx, "SET %s %d",key,ICBObject_IntVal(valueObj));
			break;
		}
	case CB_UINT:
		{
			reply = (redisReply *)redisCommand(ctx, "SET %s %u",key,(unsigned int)ICBObject_IntVal(valueObj));
			break;
		}
	case CB_LONG:
		{
			reply = (redisReply *)redisCommand(ctx, "SET %s %lld",key,ICBObject_LongVal(valueObj));
			break;
		}
	case CB_FLOAT:
		{
			reply = (redisReply *)redisCommand(ctx, "SET %s %f",key,ICBObject_FloatVal(valueObj));
			break;
		}
	case CB_BOOL:
		{
			reply = (redisReply *)redisCommand(ctx, "SET %s %s",key,ICBObject_BoolVal(valueObj) ? "true" : "false");
			break;
		}
	case CB_STRING:
		{
			int len = 0;
			const char* v = ICBObject_BytesVal(valueObj,&len);
			reply = (redisReply *)redisCommand(ctx, "SET %s %b",key,v,(size_t)len);
			break;
		}
	case CB_CLASS:
		{
			ICBObject* lenObj = ICBObjectList_GetCBObject(args,2);
			IClassObject* clsObj = ICBObject_ClassObjValue(valueObj);
			if (strcasecmp(IClassObject_GetCBClassName(clsObj),"ByteArray") != 0)
			{
				return ICBrother_CreateCBObjectBool(pCBrother,false);
			}

			if (!ICBrother_WriteLockCBClsObject(pCBrother,clsObj))
			{
				ICBrother_CreateException(pCBrother,"SyncException","multi thread access at object func! 'ByteArray' Object!");
				return ICBrother_CreateCBObjectBool(pCBrother,false);
			}

			ICBByteArray* byteArray = (ICBByteArray*)IClassObject_GetUserParm(clsObj);
			const char* data = ICBByteArray_GetBuf(byteArray) + ICBByteArray_GetReadPos(byteArray);
			size_t len = ICBByteArray_GetWritePos(byteArray) - ICBByteArray_GetReadPos(byteArray);
			if (lenObj != NULL)
			{
				len = ICBObject_AnyTypeToInt(lenObj);
			}

			reply = (redisReply *)redisCommand(ctx, "SET %s %b",key,data,len);			
			ICBrother_WriteUnlockCBClsObject(pCBrother,clsObj);
			break;
		}
	}

	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_STATUS)
	{
		freeReplyObject(reply);
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	bool suc = strcasecmp(reply->str,"OK") == 0;
	freeReplyObject(reply);
	return ICBrother_CreateCBObjectBool(pCBrother,suc);
}

ICBObject* CBRedis_Get(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_Key_Get_Value(pCBrother,args,obj,pException,"GET",false);
}

ICBObject* CBRedis_GetBytes(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_Key_Get_Value(pCBrother,args,obj,pException,"GET",true);
}

ICBObject* CBRedis_HSet(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* fieldObj = ICBObjectList_GetCBObject(args,1);
	ICBObject* valueObj = ICBObjectList_GetCBObject(args,2);
	if (keyObj == NULL || valueObj == NULL || fieldObj == NULL|| ICBObject_Type(keyObj) != CB_STRING || ICBObject_Type(fieldObj) != CB_STRING)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	const char* key = ICBObject_StringVal(keyObj);
	const char* field = ICBObject_StringVal(fieldObj);
	redisReply *reply = NULL;

	switch (ICBObject_Type(valueObj))
	{
	case CB_INT:
		{
			reply = (redisReply *)redisCommand(ctx, "HSET %s %s %d",key,field,ICBObject_IntVal(valueObj));
			break;
		}
	case CB_UINT:
		{
			reply = (redisReply *)redisCommand(ctx, "HSET %s %s %u",key,field,(unsigned int)ICBObject_IntVal(valueObj));
			break;
		}
	case CB_LONG:
		{
			reply = (redisReply *)redisCommand(ctx, "HSET %s %s %lld",key,field,ICBObject_LongVal(valueObj));
			break;
		}
	case CB_FLOAT:
		{
			reply = (redisReply *)redisCommand(ctx, "HSET %s %s %f",key,field,ICBObject_FloatVal(valueObj));
			break;
		}
	case CB_BOOL:
		{
			reply = (redisReply *)redisCommand(ctx, "HSET %s %s %s",key,field,ICBObject_BoolVal(valueObj) ? "true" : "false");
			break;
		}
	case CB_STRING:
		{
			int len = 0;
			const char* v = ICBObject_BytesVal(valueObj,&len);
			reply = (redisReply *)redisCommand(ctx, "HSET %s %s %b",key,field,v,(size_t)len);
			break;
		}
	case CB_CLASS:
		{
			ICBObject* lenObj = ICBObjectList_GetCBObject(args,2);
			IClassObject* clsObj = ICBObject_ClassObjValue(valueObj);
			if (strcasecmp(IClassObject_GetCBClassName(clsObj),"ByteArray") != 0)
			{
				return ICBrother_CreateCBObjectBool(pCBrother,false);
			}

			if (!ICBrother_WriteLockCBClsObject(pCBrother,clsObj))
			{
				ICBrother_CreateException(pCBrother,"SyncException","multi thread access at object func! 'ByteArray' Object!");
				return ICBrother_CreateCBObjectBool(pCBrother,false);
			}

			ICBByteArray* byteArray = (ICBByteArray*)IClassObject_GetUserParm(clsObj);
			const char* data = ICBByteArray_GetBuf(byteArray) + ICBByteArray_GetReadPos(byteArray);
			size_t len = ICBByteArray_GetWritePos(byteArray) - ICBByteArray_GetReadPos(byteArray);
			if (lenObj != NULL)
			{
				len = ICBObject_AnyTypeToInt(lenObj);
			}

			reply = (redisReply *)redisCommand(ctx, "HSET %s %s %b",key,field,data,len);			
			ICBrother_WriteUnlockCBClsObject(pCBrother,clsObj);
			break;
		}
	}

	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_STATUS)
	{
		freeReplyObject(reply);
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	bool suc = strcasecmp(reply->str,"OK") == 0;
	freeReplyObject(reply);
	return ICBrother_CreateCBObjectBool(pCBrother,suc);
}

ICBObject* CBRedis_HGet(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* fieldObj = ICBObjectList_GetCBObject(args,1);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING || fieldObj == NULL || ICBObject_Type(fieldObj) != CB_STRING)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	const char* key = ICBObject_StringVal(keyObj);
	const char* field = ICBObject_StringVal(fieldObj);

	redisReply *reply = (redisReply *)redisCommand(ctx, "HGET %s %s",key,field);
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_HGetBytes(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* fieldObj = ICBObjectList_GetCBObject(args,1);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING || fieldObj == NULL || ICBObject_Type(fieldObj) != CB_STRING)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	const char* key = ICBObject_StringVal(keyObj);
	const char* field = ICBObject_StringVal(fieldObj);

	redisReply *reply = (redisReply *)redisCommand(ctx, "HGET %s %s",key,field);
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply,true);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_HExists(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* fieldObj = ICBObjectList_GetCBObject(args,1);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING || fieldObj == NULL || ICBObject_Type(fieldObj) != CB_STRING)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	const char* key = ICBObject_StringVal(keyObj);
	const char* field = ICBObject_StringVal(fieldObj);

	redisReply *reply = (redisReply *)redisCommand(ctx, "HEXISTS %s %s",key,field);
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	ICBObject* resObj = ICBrother_CreateCBObjectBool(pCBrother,reply->integer == 1);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_HLen(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_OneKey_Interger(pCBrother,args,obj,pException,"HLEN");
}

ICBObject* CBRedis_HKeys(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING)
	{
		return ICBrother_CreateCBClassObject(pCBrother,"Array",NULL,0);
	}

	const char* key = ICBObject_StringVal(keyObj);

	redisReply *reply = (redisReply *)redisCommand(ctx, "HKEYS %s",key);
	if (reply == NULL)
	{
		return ICBrother_CreateCBClassObject(pCBrother,"Array",NULL,0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_HVals(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING)
	{
		return ICBrother_CreateCBClassObject(pCBrother,"Array",NULL,0);
	}

	const char* key = ICBObject_StringVal(keyObj);

	redisReply *reply = (redisReply *)redisCommand(ctx, "HVALS %s",key);
	if (reply == NULL)
	{
		return ICBrother_CreateCBClassObject(pCBrother,"Array",NULL,0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_HGetAll(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	const char* key = ICBObject_StringVal(keyObj);

	redisReply *reply = (redisReply *)redisCommand(ctx, "HGETALL %s",key);
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_HDel(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	const char* key = ICBObject_StringVal(keyObj);

	string fieldStr = "HDEL ";
	fieldStr += key;
	for (int i = 1 ; i < ICBObjectList_Size(args) ; i++)
	{
		ICBObject* fieldObj = ICBObjectList_GetCBObject(args,i);
		if (fieldObj == NULL || ICBObject_Type(fieldObj) != CB_STRING)
		{
			return ICBrother_CreateCBObjectInt(pCBrother,0);
		}
		fieldStr += " ";
		fieldStr += ICBObject_StringVal(fieldObj);
	}

	redisReply *reply = (redisReply *)redisCommand(ctx, fieldStr.c_str());
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_LLen(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_OneKey_Interger(pCBrother,args,obj,pException,"LLEN");
}

ICBObject* CBRedis_LRange(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* beginObj = ICBObjectList_GetCBObject(args,1);
	ICBObject* endObj = ICBObjectList_GetCBObject(args,2);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING || beginObj == NULL || endObj == NULL)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	const char* key = ICBObject_StringVal(keyObj);
	int beginPos = ICBObject_AnyTypeToInt(beginObj);
	int endPos = ICBObject_AnyTypeToInt(endObj);
	redisReply *reply = (redisReply *)redisCommand(ctx, "LRANGE %s %d %d", key,beginPos,endPos);
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_LRpush(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException,const char* cmd)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* valueObj = ICBObjectList_GetCBObject(args,1);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING || valueObj == NULL)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,-1);
	}

	const char* key = ICBObject_StringVal(keyObj);
	redisReply *reply = NULL;
	switch (ICBObject_Type(valueObj))
	{
	case CB_INT:
	case CB_UINT:
	case CB_LONG:
	case CB_FLOAT:
	case CB_BOOL:
		{
			char buf[64] = {0};
			reply = (redisReply *)redisCommand(ctx, "%s %s %s",cmd,key,ICBObject_AnyTypeToString(valueObj,buf));
			break;
		}
	case CB_STRING:
		{
			int len = 0;
			const char* v = ICBObject_BytesVal(valueObj,&len);
			reply = (redisReply *)redisCommand(ctx, "%s %s %b",cmd,key,v,(size_t)len);
			break;
		}
	case CB_CLASS:
		{
			ICBObject* lenObj = ICBObjectList_GetCBObject(args,3);
			IClassObject* clsObj = ICBObject_ClassObjValue(valueObj);
			if (strcasecmp(IClassObject_GetCBClassName(clsObj),"ByteArray") != 0)
			{
				return ICBrother_CreateCBObjectInt(pCBrother,-1);
			}

			if (!ICBrother_WriteLockCBClsObject(pCBrother,clsObj))
			{
				ICBrother_CreateException(pCBrother,"SyncException","multi thread access at object func! 'ByteArray' Object!");
				return NULL;
			}

			ICBByteArray* byteArray = (ICBByteArray*)IClassObject_GetUserParm(clsObj);
			const char* data = ICBByteArray_GetBuf(byteArray) + ICBByteArray_GetReadPos(byteArray);
			size_t len = ICBByteArray_GetWritePos(byteArray) - ICBByteArray_GetReadPos(byteArray);
			if (lenObj != NULL)
			{
				len = ICBObject_AnyTypeToInt(lenObj);
			}

			reply = (redisReply *)redisCommand(ctx, "%s %s %b",cmd,key,data,len);
			ICBrother_WriteUnlockCBClsObject(pCBrother,clsObj);
			break;
		}
	}

	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,-1);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_RPush(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_LRpush(pCBrother,args,obj,pException,"RPUSH");
}

ICBObject* CBRedis_LPush(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_LRpush(pCBrother,args,obj,pException,"LPUSH");
}

ICBObject* CBRedis_LPop(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	const char* key = ICBObject_StringVal(keyObj);
	redisReply *reply = (redisReply *)redisCommand(ctx, "LPOP %s", key);
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_RPop(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	const char* key = ICBObject_StringVal(keyObj);
	redisReply *reply = (redisReply *)redisCommand(ctx, "RPOP %s", key);
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_LRem(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* countObj = ICBObjectList_GetCBObject(args,1);
	ICBObject* valueObj = ICBObjectList_GetCBObject(args,2);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING || valueObj == NULL || countObj == NULL)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	const char* key = ICBObject_StringVal(keyObj);
	int count = ICBObject_AnyTypeToInt(countObj);
	redisReply *reply = NULL;
	switch (ICBObject_Type(valueObj))
	{
	case CB_INT:
	case CB_UINT:
	case CB_LONG:
	case CB_FLOAT:
	case CB_BOOL:
		{
			char buf[64] = {0};
			reply = (redisReply *)redisCommand(ctx, "LREM %s %d %s",key,count,ICBObject_AnyTypeToString(valueObj,buf));
			break;
		}
	case CB_STRING:
		{
			int len = 0;
			const char* v = ICBObject_BytesVal(valueObj,&len);
			reply = (redisReply *)redisCommand(ctx, "LREM %s %d %b",key,count,v,(size_t)len);
			break;
		}
	case CB_CLASS:
		{
			ICBObject* lenObj = ICBObjectList_GetCBObject(args,3);
			IClassObject* clsObj = ICBObject_ClassObjValue(valueObj);
			if (strcasecmp(IClassObject_GetCBClassName(clsObj),"ByteArray") != 0)
			{
				return ICBrother_CreateCBObjectInt(pCBrother,-1);
			}

			if (!ICBrother_WriteLockCBClsObject(pCBrother,clsObj))
			{
				ICBrother_CreateException(pCBrother,"SyncException","multi thread access at object func! 'ByteArray' Object!");
				return NULL;
			}

			ICBByteArray* byteArray = (ICBByteArray*)IClassObject_GetUserParm(clsObj);
			const char* data = ICBByteArray_GetBuf(byteArray) + ICBByteArray_GetReadPos(byteArray);
			size_t len = ICBByteArray_GetWritePos(byteArray) - ICBByteArray_GetReadPos(byteArray);
			if (lenObj != NULL)
			{
				len = ICBObject_AnyTypeToInt(lenObj);
			}

			reply = (redisReply *)redisCommand(ctx, "LREM %s %d %b",key,count,data,len);
			ICBrother_WriteUnlockCBClsObject(pCBrother,clsObj);
			break;
		}
	}

	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_LIndex(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* posObj = ICBObjectList_GetCBObject(args,1);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING || posObj == NULL)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	const char* key = ICBObject_StringVal(keyObj);
	int pos = ICBObject_AnyTypeToInt(posObj);
	redisReply *reply = (redisReply *)redisCommand(ctx, "LINDEX %s %d", key,pos);
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_LInsert(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException,bool isBefore)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* value1Obj = ICBObjectList_GetCBObject(args,1);
	ICBObject* value2Obj = ICBObjectList_GetCBObject(args,2);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING || value1Obj == NULL || value2Obj == NULL)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,-1);
	}

	const char* key = ICBObject_StringVal(keyObj);
	redisReply *reply = NULL;

	switch (ICBObject_Type(value1Obj))
	{
	case CB_INT:
	case CB_UINT:
	case CB_LONG:
	case CB_FLOAT:
	case CB_BOOL:
		{
			char buf[64] = {0};
			const char* value1 = ICBObject_AnyTypeToString(value1Obj,buf);
			switch (ICBObject_Type(value2Obj))
			{
			case CB_INT:
			case CB_UINT:
			case CB_LONG:
			case CB_FLOAT:
			case CB_BOOL:
				{
					char buf2[64] = {0};
					const char* value2 = ICBObject_AnyTypeToString(value2Obj,buf2);
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
					const char* v = ICBObject_BytesVal(value2Obj,&len);
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
					ICBObject* lenObj = ICBObjectList_GetCBObject(args,3);
					IClassObject* clsObj = ICBObject_ClassObjValue(value2Obj);
					if (strcasecmp(IClassObject_GetCBClassName(clsObj),"ByteArray") != 0)
					{
						return ICBrother_CreateCBObjectInt(pCBrother,-1);
					}

					if (!ICBrother_WriteLockCBClsObject(pCBrother,clsObj))
					{
						ICBrother_CreateException(pCBrother,"SyncException","multi thread access at object func! 'ByteArray' Object!");
						return NULL;
					}

					ICBByteArray* byteArray = (ICBByteArray*)IClassObject_GetUserParm(clsObj);
					const char* data = ICBByteArray_GetBuf(byteArray) + ICBByteArray_GetReadPos(byteArray);
					size_t len = ICBByteArray_GetWritePos(byteArray) - ICBByteArray_GetReadPos(byteArray);
					if (lenObj != NULL)
					{
						len = ICBObject_AnyTypeToInt(lenObj);
					}

					if (isBefore)
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s BEFORE %s %b",key,value1,data,len);
					}
					else
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s AFTER %s %b",key,value1,data,len);
					}
					ICBrother_WriteUnlockCBClsObject(pCBrother,clsObj);
					break;
				}
			}			
			
			break;
		}
	case CB_STRING:
		{
			int len1 = 0;
			const char* value1 = ICBObject_BytesVal(value1Obj,&len1);
			switch (ICBObject_Type(value2Obj))
			{
			case CB_INT:
			case CB_UINT:
			case CB_LONG:
			case CB_FLOAT:
			case CB_BOOL:
				{
					char buf2[64] = {0};
					const char* value2 = ICBObject_AnyTypeToString(value2Obj,buf2);
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
					const char* v = ICBObject_BytesVal(value2Obj,&len);
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
					ICBObject* lenObj = ICBObjectList_GetCBObject(args,3);
					IClassObject* clsObj = ICBObject_ClassObjValue(value2Obj);
					if (strcasecmp(IClassObject_GetCBClassName(clsObj),"ByteArray") != 0)
					{
						return ICBrother_CreateCBObjectInt(pCBrother,-1);
					}

					if (!ICBrother_WriteLockCBClsObject(pCBrother,clsObj))
					{
						ICBrother_CreateException(pCBrother,"SyncException","multi thread access at object func! 'ByteArray' Object!");
						return NULL;
					}

					ICBByteArray* byteArray = (ICBByteArray*)IClassObject_GetUserParm(clsObj);
					const char* data = ICBByteArray_GetBuf(byteArray) + ICBByteArray_GetReadPos(byteArray);
					size_t len = ICBByteArray_GetWritePos(byteArray) - ICBByteArray_GetReadPos(byteArray);
					if (lenObj != NULL)
					{
						len = ICBObject_AnyTypeToInt(lenObj);
					}

					if (isBefore)
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s BEFORE %b %b",key,value1,(size_t)len1,data,len);
					}
					else
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s AFTER %b %b",key,value1,(size_t)len1,data,len);
					}
					ICBrother_WriteUnlockCBClsObject(pCBrother,clsObj);
					break;
				}
			}
			break;
		}
	case CB_CLASS:
		{
			IClassObject* clsObj1 = ICBObject_ClassObjValue(value1Obj);
			if (strcasecmp(IClassObject_GetCBClassName(clsObj1),"ByteArray") != 0)
			{
				return ICBrother_CreateCBObjectInt(pCBrother,-1);
			}

			if (!ICBrother_WriteLockCBClsObject(pCBrother,clsObj1))
			{
				ICBrother_CreateException(pCBrother,"SyncException","multi thread access at object func! 'ByteArray' Object!");
				return NULL;
			}

			ICBByteArray* byteArray1 = (ICBByteArray*)IClassObject_GetUserParm(clsObj1);
			const char* value1 = ICBByteArray_GetBuf(byteArray1) + ICBByteArray_GetReadPos(byteArray1);
			size_t len1 = ICBByteArray_GetWritePos(byteArray1) - ICBByteArray_GetReadPos(byteArray1);

			switch (ICBObject_Type(value2Obj))
			{
			case CB_INT:
			case CB_UINT:
			case CB_LONG:
			case CB_FLOAT:
			case CB_BOOL:
				{
					char buf2[64] = {0};
					const char* value2 = ICBObject_AnyTypeToString(value2Obj,buf2);
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
					const char* v = ICBObject_BytesVal(value2Obj,&len);
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
					ICBObject* lenObj = ICBObjectList_GetCBObject(args,3);
					IClassObject* clsObj = ICBObject_ClassObjValue(value2Obj);
					if (strcasecmp(IClassObject_GetCBClassName(clsObj),"ByteArray") != 0)
					{
						ICBrother_WriteUnlockCBClsObject(pCBrother,clsObj1);
						return ICBrother_CreateCBObjectInt(pCBrother,-1);
					}

					if (!ICBrother_WriteLockCBClsObject(pCBrother,clsObj))
					{
						ICBrother_WriteUnlockCBClsObject(pCBrother,clsObj1);
						ICBrother_CreateException(pCBrother,"SyncException","multi thread access at object func! 'ByteArray' Object!");
						return NULL;
					}

					ICBByteArray* byteArray = (ICBByteArray*)IClassObject_GetUserParm(clsObj);
					const char* data = ICBByteArray_GetBuf(byteArray) + ICBByteArray_GetReadPos(byteArray);
					size_t len = ICBByteArray_GetWritePos(byteArray) - ICBByteArray_GetReadPos(byteArray);
					if (lenObj != NULL)
					{
						len = ICBObject_AnyTypeToInt(lenObj);
					}

					if (isBefore)
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s BEFORE %b %b",key,value1,len1,data,len);
					}
					else
					{
						reply = (redisReply *)redisCommand(ctx, "LINSERT %s AFTER %b %b",key,value1,len1,data,len);
					}
					ICBrother_WriteUnlockCBClsObject(pCBrother,clsObj);
					break;
				}
			}

			ICBrother_WriteUnlockCBClsObject(pCBrother,clsObj1);
			break;
		}
	}

	if (reply == NULL)
	{
		if (ctx->err != 0)
		{
			*pException = ICBrother_CreateException(pCBrother,"RedisException", ctx->errstr);
		}
		return NULL;
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_LInsertBefore(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_LInsert(pCBrother,args,obj,pException,true);
}

ICBObject* CBRedis_LInsertAfter(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_LInsert(pCBrother,args,obj,pException,false);
}

ICBObject* CBRedis_LSet(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* countObj = ICBObjectList_GetCBObject(args,1);
	ICBObject* valueObj = ICBObjectList_GetCBObject(args,2);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING || valueObj == NULL || countObj == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	const char* key = ICBObject_StringVal(keyObj);
	int count = ICBObject_AnyTypeToInt(countObj);
	redisReply *reply = NULL;
	switch (ICBObject_Type(valueObj))
	{
	case CB_INT:
	case CB_UINT:
	case CB_LONG:
	case CB_FLOAT:
	case CB_BOOL:
		{
			char buf[64] = {0};
			reply = (redisReply *)redisCommand(ctx, "LSET %s %d %s",key,count,ICBObject_AnyTypeToString(valueObj,buf));
			break;
		}
	case CB_STRING:
		{
			int len = 0;
			const char* v = ICBObject_BytesVal(valueObj,&len);
			reply = (redisReply *)redisCommand(ctx, "LSET %s %d %b",key,count,v,(size_t)len);
			break;
		}
	case CB_CLASS:
		{
			ICBObject* lenObj = ICBObjectList_GetCBObject(args,3);
			IClassObject* clsObj = ICBObject_ClassObjValue(valueObj);
			if (strcasecmp(IClassObject_GetCBClassName(clsObj),"ByteArray") != 0)
			{
				return ICBrother_CreateCBObjectInt(pCBrother,-1);
			}

			if (!ICBrother_WriteLockCBClsObject(pCBrother,clsObj))
			{
				ICBrother_CreateException(pCBrother,"SyncException","multi thread access at object func! 'ByteArray' Object!");
				return NULL;
			}

			ICBByteArray* byteArray = (ICBByteArray*)IClassObject_GetUserParm(clsObj);
			const char* data = ICBByteArray_GetBuf(byteArray) + ICBByteArray_GetReadPos(byteArray);
			size_t len = ICBByteArray_GetWritePos(byteArray) - ICBByteArray_GetReadPos(byteArray);
			if (lenObj != NULL)
			{
				len = ICBObject_AnyTypeToInt(lenObj);
			}

			reply = (redisReply *)redisCommand(ctx, "LSET %s %d %b",key,count,data,len);
			ICBrother_WriteUnlockCBClsObject(pCBrother,clsObj);
			break;
		}
	}

	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_STATUS)
	{
		freeReplyObject(reply);
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	bool suc = strcasecmp(reply->str,"OK") == 0;
	freeReplyObject(reply);
	return ICBrother_CreateCBObjectBool(pCBrother,suc);
}

ICBObject* CBRedis_Command_Integer(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException,const char* cmd)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	redisReply *reply = (redisReply *)redisCommand(ctx, cmd);

	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_DbSize(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_Integer(pCBrother,args,obj,pException,"DBSIZE");
}

ICBObject* CBRedis_LastSave(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_Integer(pCBrother,args,obj,pException,"LASTSAVE");
}

ICBObject* CBRedis_BgSave(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	redisReply *reply = (redisReply *)redisCommand(ctx, "BGSAVE");

	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_STATUS)
	{
		freeReplyObject(reply);
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	freeReplyObject(reply);
	bool suc = strcasecmp(reply->str,"Background saving started") == 0;
	freeReplyObject(reply);
	return ICBrother_CreateCBObjectBool(pCBrother,suc);
}

ICBObject* CBRedis_Save(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_OK(pCBrother,args,obj,pException,"SAVE");
}

ICBObject* CBRedis_FlushAll(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_OK(pCBrother,args,obj,pException,"FLUSHALL");
}

ICBObject* CBRedis_FlushDB(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_OK(pCBrother,args,obj,pException,"FLUSHDB");
}

ICBObject* CBRedis_SAdd(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_KeyValue_Bool(pCBrother,args,obj,pException,"SADD");
}

ICBObject* CBRedis_SCard(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_OneKey_Interger(pCBrother,args,obj,pException,"SCARD");
}

ICBObject* CBRedis_SRem(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_KeyValue_Bool(pCBrother,args,obj,pException,"SREM");
}

ICBObject* CBRedis_SMembers(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_Key_Get_Value(pCBrother,args,obj,pException,"SMEMBERS",false);
}

ICBObject* CBRedis_Command_Key_Cnt_Value(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException,const char* cmd)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* cntObj = ICBObjectList_GetCBObject(args,1);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	const char* key = ICBObject_StringVal(keyObj);
	int cnt = -1;
	if (cntObj != NULL)
	{
		cnt = ICBObject_AnyTypeToInt(cntObj);
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
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_SPop(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_Key_Cnt_Value(pCBrother,args,obj,pException,"SPOP");
}

ICBObject* CBRedis_SRandmember(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_Key_Cnt_Value(pCBrother,args,obj,pException,"SRANDMEMBER");
}

ICBObject* CBRedis_SInter(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_Key_Field_Get_Value(pCBrother,args,obj,pException,"SINTER",false);
}

ICBObject* CBRedis_SUnion(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_Key_Field_Get_Value(pCBrother,args,obj,pException,"SUNION",false);
}

ICBObject* CBRedis_SDiff(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_Key_Field_Get_Value(pCBrother,args,obj,pException,"SDIFF",false);
}

ICBObject* CBRedis_ZAdd(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* scoreObj = ICBObjectList_GetCBObject(args,1);
	ICBObject* valueObj = ICBObjectList_GetCBObject(args,2);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING || scoreObj == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	const char* key = ICBObject_StringVal(keyObj);
	int score = ICBObject_AnyTypeToInt(scoreObj);
	redisReply *reply = NULL;
	switch (ICBObject_Type(valueObj))
	{
	case CB_INT:
	case CB_UINT:
	case CB_LONG:
	case CB_FLOAT:
	case CB_BOOL:
		{
			char buf[64] = {0};
			reply = (redisReply *)redisCommand(ctx, "ZADD %s %d %s",key,score,ICBObject_AnyTypeToString(valueObj,buf));
			break;
		}
	case CB_STRING:
		{
			int len = 0;
			const char* v = ICBObject_BytesVal(valueObj,&len);
			reply = (redisReply *)redisCommand(ctx, "ZADD %s %d %b",key,score,v,(size_t)len);
			break;
		}
	case CB_CLASS:
		{
			ICBObject* lenObj = ICBObjectList_GetCBObject(args,3);
			IClassObject* clsObj = ICBObject_ClassObjValue(valueObj);
			if (strcasecmp(IClassObject_GetCBClassName(clsObj),"ByteArray") != 0)
			{
				return ICBrother_CreateCBObjectBool(pCBrother,false);
			}

			if (!ICBrother_WriteLockCBClsObject(pCBrother,clsObj))
			{
				ICBrother_CreateException(pCBrother,"SyncException","multi thread access at object func! 'ByteArray' Object!");
				return NULL;
			}

			ICBByteArray* byteArray = (ICBByteArray*)IClassObject_GetUserParm(clsObj);
			const char* data = ICBByteArray_GetBuf(byteArray) + ICBByteArray_GetReadPos(byteArray);
			size_t len = ICBByteArray_GetWritePos(byteArray) - ICBByteArray_GetReadPos(byteArray);
			if (lenObj != NULL)
			{
				len = ICBObject_AnyTypeToInt(lenObj);
			}

			reply = (redisReply *)redisCommand(ctx, "ZADD %s %d %b",key,score,data,len);
			ICBrother_WriteUnlockCBClsObject(pCBrother,clsObj);
			break;
		}
	}

	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return ICBrother_CreateCBObjectBool(pCBrother,false);
	}

	bool suc = reply->integer;
	freeReplyObject(reply);
	return ICBrother_CreateCBObjectBool(pCBrother,suc);
}

ICBObject* CBRedis_ZCard(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_OneKey_Interger(pCBrother,args,obj,pException,"ZCARD");
}

ICBObject* CBRedis_ZCount(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* beginObj = ICBObjectList_GetCBObject(args,1);
	ICBObject* endObj = ICBObjectList_GetCBObject(args,2);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING || beginObj == NULL || endObj == NULL)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	const char* key = ICBObject_StringVal(keyObj);
	int beginPos = ICBObject_AnyTypeToInt(beginObj);
	int endPos = ICBObject_AnyTypeToInt(endObj);
	redisReply *reply = (redisReply *)redisCommand(ctx, "ZCOUNT %s %d %d", key,beginPos,endPos);
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	if (reply->type != REDIS_REPLY_INTEGER)
	{
		freeReplyObject(reply);
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_ZRange(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* beginObj = ICBObjectList_GetCBObject(args,1);
	ICBObject* endObj = ICBObjectList_GetCBObject(args,2);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING || beginObj == NULL || endObj == NULL)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	const char* key = ICBObject_StringVal(keyObj);
	int beginPos = ICBObject_AnyTypeToInt(beginObj);
	int endPos = ICBObject_AnyTypeToInt(endObj);
	redisReply *reply = (redisReply *)redisCommand(ctx, "ZRANGE %s %d %d WITHSCORES", key,beginPos,endPos);
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_ZRevrange(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	redisContext* ctx = (redisContext*)IClassObject_GetUserParm(obj);
	if (ctx == NULL)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException","redis is not connected.");
		return NULL;
	}

	ICBObject* keyObj = ICBObjectList_GetCBObject(args,0);
	ICBObject* beginObj = ICBObjectList_GetCBObject(args,1);
	ICBObject* endObj = ICBObjectList_GetCBObject(args,2);
	if (keyObj == NULL || ICBObject_Type(keyObj) != CB_STRING || beginObj == NULL || endObj == NULL)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	const char* key = ICBObject_StringVal(keyObj);
	int beginPos = ICBObject_AnyTypeToInt(beginObj);
	int endPos = ICBObject_AnyTypeToInt(endObj);
	redisReply *reply = (redisReply *)redisCommand(ctx, "ZREVRANGE %s %d %d WITHSCORES", key,beginPos,endPos);
	if (reply == NULL)
	{
		return ICBrother_CreateCBObjectInt(pCBrother,0);
	}

	if (reply->type == REDIS_REPLY_ERROR)
	{
		*pException = ICBrother_CreateException(pCBrother,"RedisException",reply->str);
		freeReplyObject(reply);
		return NULL;
	}

	ICBObject* resObj = ReplyToCBObject(pCBrother,reply);
	freeReplyObject(reply);
	return resObj;
}

ICBObject* CBRedis_ZScore(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_Key_Field_Get_Value(pCBrother,args,obj,pException,"ZSCORE",false);
}

ICBObject* CBRedis_ZRank(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_Key_Field_Get_Value(pCBrother,args,obj,pException,"ZRANK",false);
}

ICBObject* CBRedis_ZRevrank(ICBrother* pCBrother,ICBObjectList* args,IClassObject* obj,ICBException** pException)
{
	return CBRedis_Command_Key_Field_Get_Value(pCBrother,args,obj,pException,"ZREVRANK",false);
}

bool Init(ICBrother* pCBrother)
{
	//regiest your function and class
	ICBrother_RegisterCBModuleClass(pCBrother,"Client",CBRedis_Init,CBRedis_Release,"redis");

	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","connect",CBRedis_Connect,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","close",CBRedis_Close,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","auth",CBRedis_Auth,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","select",CBRedis_Select,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","ping",CBRedis_Ping,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","dbSize",CBRedis_DbSize,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","bgSave",CBRedis_BgSave,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","save",CBRedis_Save,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","flushAll",CBRedis_FlushAll,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","flushDB",CBRedis_FlushDB,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","lastSave",CBRedis_LastSave,true);

	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","exists",CBRedis_Exists,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","delete",CBRedis_Delete,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","expire",CBRedis_Expire,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","pexpire",CBRedis_PExpire,true);	
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","persist",CBRedis_Persist,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","move",CBRedis_Move,true);	
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","ttl",CBRedis_TTL,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","pttl",CBRedis_PTTL,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","strlen",CBRedis_Strlen,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","type",CBRedis_Type,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","set",CBRedis_Set,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","get",CBRedis_Get,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","getBytes",CBRedis_GetBytes,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","incr",CBRedis_Incr,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","incrBy",CBRedis_IncrBy,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","incrByFloat",CBRedis_IncrByFloat,true);	
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","decr",CBRedis_Decr,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","decrBy",CBRedis_DecrBy,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","append",CBRedis_Append,true);	

	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","hset",CBRedis_HSet,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","hget",CBRedis_HGet,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","hgetBytes",CBRedis_HGetBytes,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","hexists",CBRedis_HExists,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","hlen",CBRedis_HLen,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","hkeys",CBRedis_HKeys,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","hvals",CBRedis_HVals,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","hgetAll",CBRedis_HGetAll,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","hdel",CBRedis_HDel,true);

	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","llen",CBRedis_LLen,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","lrange",CBRedis_LRange,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","rpush",CBRedis_RPush,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","lpush",CBRedis_LPush,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","lindex",CBRedis_LIndex,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","linsertAfter",CBRedis_LInsertAfter,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","linsertBefore",CBRedis_LInsertBefore,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","lpop",CBRedis_LPop,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","rpop",CBRedis_RPop,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","lrem",CBRedis_LRem,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","lset",CBRedis_LSet,true);
	
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","sadd",CBRedis_SAdd,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","scard",CBRedis_SCard,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","srem",CBRedis_SRem,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","smembers",CBRedis_SMembers,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","spop",CBRedis_SPop,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","srandMember",CBRedis_SRandmember,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","sinter",CBRedis_SInter,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","sunion",CBRedis_SUnion,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","sdiff",CBRedis_SDiff,true);

	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","zadd",CBRedis_ZAdd,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","zcard",CBRedis_ZCard,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","zcount",CBRedis_ZCount,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","zrange",CBRedis_ZRange,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","zrevrange",CBRedis_ZRevrange,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","zscore",CBRedis_ZScore,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","zrank",CBRedis_ZRank,true);
	ICBrother_RegisterCBClassFunc(pCBrother,"redis::Client","zrevrank",CBRedis_ZRevrank,true);	
	
	return true;
}
CREATE_CBROTHER_MODULE(Init)
