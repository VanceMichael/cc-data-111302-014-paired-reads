import http from 'node:http';
import {readFile} from 'node:fs/promises';
import {createHash} from 'node:crypto';
import { gzipSync } from 'node:zlib';
export function json(response,value,status=200){response.writeHead(status,{'Content-Type':'application/json; charset=utf-8'});response.end(JSON.stringify(value));}
export async function createSource(){
  const state=JSON.parse(await readFile(new URL('../fixtures/source.json',import.meta.url),'utf8'));
  
  const pages={};
  const server=http.createServer(async(request,response)=>{
    try{
      const url=new URL(request.url,'http://local.test');let text='';
      for await(const chunk of request){text+=chunk;if(text.length>8*1024*1024){json(response,{error:'控制请求过大'},413);return;}}
      let body={};if(text){try{body=JSON.parse(text);}catch{return json(response,{error:'JSON无效'},400);}}
      if(url.pathname==='/health')return json(response,{status:'ok'});
      if(url.pathname==='/state'||url.pathname==='/inventory')return json(response,state);
      if(url.pathname==='/control'&&request.method==='POST'){Object.assign(state,body); return json(response,state);}
      if(Object.hasOwn(pages,url.pathname)){response.setHeader('Content-Type','text/html; charset=utf-8');return response.end(await readFile(new URL('../public/'+pages[url.pathname],import.meta.url)));}
      
    if(url.pathname==='/manifest')return json(response,{run:'run1',lanes:[{id:'lane1',r1:'lane1-R1.gz',r2:'lane1-R2.gz',count:2},{id:'lane2',r1:'lane2-R1.gz',r2:'lane2-R2.gz',count:1}]});
    if(url.pathname.startsWith('/files/')){const name=url.pathname.split('/').pop();let text;if(name==='lane1-R1.gz')text=['@read-a/1\nAC\nGT\n+\n!!\n!!\n','@read-b/1\nAG\n+\n##\n'];else if(name==='lane1-R2.gz')text=['@read-b 2:N:0:1\nCT\n+\n##\n','@read-a/2\nACGT\n+\n!!!!\n'];else if(name==='lane2-R1.gz')text=['@read-a/1\nAA\n+\n!!\n'];else if(name==='lane2-R2.gz')text=['@read-a/2\nTT\n+\n!!\n'];else return json(response,{error:'未知端文件'},404);let data=Buffer.concat(text.map(value=>gzipSync(value)));if(state.badTail)data=data.subarray(0,data.length-8);response.setHeader('Content-Type','application/gzip');for(let offset=0;offset<data.length;offset+=state.chunkBytes)response.write(data.subarray(offset,offset+state.chunkBytes));return response.end();}

      return json(response,{error:'资源不存在'},404);
    }catch(error){if(!response.headersSent)json(response,{error:error.message},500);else response.destroy();}
  });
  await new Promise(resolve=>server.listen(0,'0.0.0.0',resolve));
  return {url:`http://127.0.0.1:${server.address().port}`,state,close:async()=>{server.closeAllConnections();await new Promise(resolve=>server.close(resolve));}};
}
