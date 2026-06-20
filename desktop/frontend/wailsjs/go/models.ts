export namespace main {
	
	export class DesktopConfig {
	    data_dir: string;
	    setup_complete: boolean;
	    grpc_addr: string;
	    api_addr: string;
	    gateway_addr: string;
	    keep_alive: boolean;
	    auto_start: boolean;
	
	    static createFrom(source: any = {}) {
	        return new DesktopConfig(source);
	    }
	
	    constructor(source: any = {}) {
	        if ('string' === typeof source) source = JSON.parse(source);
	        this.data_dir = source["data_dir"];
	        this.setup_complete = source["setup_complete"];
	        this.grpc_addr = source["grpc_addr"];
	        this.api_addr = source["api_addr"];
	        this.gateway_addr = source["gateway_addr"];
	        this.keep_alive = source["keep_alive"];
	        this.auto_start = source["auto_start"];
	    }
	}

}

