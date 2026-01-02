// static/js/task-poller.js
class TaskPoller {
    constructor(taskId, options = {}) {
        this.taskId = taskId;
        this.pollInterval = options.pollInterval || 5000;
        this.maxPolls = options.maxPolls || 360;
        this.pollCount = 0;
        this.intervalId = null;
        
        this.onProgress = options.onProgress || this.defaultOnProgress;
        this.onSuccess = options.onSuccess || this.defaultOnSuccess;
        this.onError = options.onError || this.defaultOnError;
        this.onTimeout = options.onTimeout || this.defaultOnTimeout;
    }
    
    start() {
        this.intervalId = setInterval(() => this.poll(), this.pollInterval);
        this.poll(); // Check immediately
    }
    
    stop() {
        if (this.intervalId) {
            clearInterval(this.intervalId);
            this.intervalId = null;
        }
    }
    
    async poll() {
        this.pollCount++;
        
        if (this.pollCount > this.maxPolls) {
            this.stop();
            this.onTimeout();
            return;
        }
        
        try {
            const response = await fetch(`/tasks/${this.taskId}/status/`);
            const data = await response.json();
            
            if (data.ready) {
                this.stop();
                if (data.success) {
                    this.onSuccess(data);
                } else {
                    this.onError(data.error || 'Task failed');
                }
            } else {
                this.onProgress(data);
            }
        } catch (error) {
            console.error('Poll error:', error);
            // Don't stop polling on network errors
        }
    }
    
    // Default handlers (can be overridden)
    defaultOnProgress(data) {
        console.log('Progress:', data.progress || 0);
    }
    
    defaultOnSuccess(data) {
        console.log('Success:', data);
        if (data.redirect_url) {
            window.location.href = data.redirect_url;
        }
    }
    
    defaultOnError(error) {
        console.error('Task failed:', error);
        alert('Task failed: ' + error);
    }
    
    defaultOnTimeout() {
        console.error('Task timed out');
        alert('Task timed out. Please refresh to check status.');
    }
}