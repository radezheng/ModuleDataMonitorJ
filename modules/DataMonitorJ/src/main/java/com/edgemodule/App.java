package com.edgemodule;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.microsoft.azure.sdk.iot.device.*;
import com.microsoft.azure.sdk.iot.device.transport.IotHubConnectionStatus;
import org.json.JSONObject;

import com.microsoft.azure.sdk.iot.device.DeviceTwin.DeviceMethodCallback;
import com.microsoft.azure.sdk.iot.device.DeviceTwin.DeviceMethodData;
import com.microsoft.azure.sdk.iot.device.DeviceTwin.Pair;
import com.microsoft.azure.sdk.iot.device.DeviceTwin.Property;
import com.microsoft.azure.sdk.iot.device.DeviceTwin.TwinPropertyCallBack;
import com.microsoft.azure.sdk.iot.device.DeviceTwin.*;


import java.util.concurrent.atomic.AtomicLong;

public class App {
    private static MessageCallbackMqtt msgCallback = new MessageCallbackMqtt();
    private static EventCallback eventCallback = new EventCallback();
    private static final String INPUT_NAME = "input1";
    private static final String OUTPUT_NAME = "output1";

    private static MonitorAgent ma = new MonitorAgent();
    
    private static final String TEMP_THRESHOLD = "TemperatureThreshold";
    private static AtomicLong tempThreshold = new AtomicLong(25);

                // Define method response codes
                private static final int METHOD_SUCCESS = 200;
                private static final int METHOD_NOT_DEFINED = 404;
                private static final int INVALID_PARAMETER = 400;

                
    protected static class EventCallback implements IotHubEventCallback {
        @Override
        public void execute(IotHubStatusCode status, Object context) {
            if (context instanceof Message) {
                System.out.println("Send message with status: " + status.name());
            } else {
                System.out.println("Invalid context passed");
            }
        }
    }

    protected static class MessageCallbackMqtt implements MessageCallback {
        private int counter = 0;

        @Override
        public IotHubMessageResult execute(Message msg, Object context) {
            this.counter += 1;

            System.out.println(
                    String.format("Received message %d: %s",
                            this.counter, new String(msg.getBytes(), Message.DEFAULT_IOTHUB_MESSAGE_CHARSET)));
            List<HashMap<String, String>> res = ma.getDataChanged(null);
            if (context instanceof ModuleClient) {
                ModuleClient client = (ModuleClient) context;

                for (int i = 0; i < res.size(); i++) {

                    JSONObject json = new JSONObject(res.get(i));
                    System.out.println(json.toString());
                    Message message = new Message(json.toString().getBytes());
                    client.sendEventAsync(message, eventCallback, null, App.OUTPUT_NAME);
                }

                // client.sendEventAsync(msg, eventCallback, msg, App.OUTPUT_NAME);
            }
            return IotHubMessageResult.COMPLETE;
        }
    }

    protected static class ConnectionStatusChangeCallback implements IotHubConnectionStatusChangeCallback {

        @Override
        public void execute(IotHubConnectionStatus status,
                IotHubConnectionStatusChangeReason statusChangeReason,
                Throwable throwable, Object callbackContext) {
            String statusStr = "Connection Status: %s";
            switch (status) {
                case CONNECTED:
                    System.out.println(String.format(statusStr, "Connected"));
                    break;
                case DISCONNECTED:
                    System.out.println(String.format(statusStr, "Disconnected"));
                    if (throwable != null) {
                        throwable.printStackTrace();
                    }
                    System.exit(1);
                    break;
                case DISCONNECTED_RETRYING:
                    System.out.println(String.format(statusStr, "Retrying"));
                    break;
                default:
                    break;
            }
        }
    }

    public static void main(String[] args) {
        try {
            IotHubClientProtocol protocol = IotHubClientProtocol.MQTT;
            System.out.println("Start to create client with MQTT protocol");
            ModuleClient client = ModuleClient.createFromEnvironment(protocol);
            System.out.println("Client created");
            client.setMessageCallback(App.INPUT_NAME, msgCallback, client);
            client.registerConnectionStatusChangeCallback(new ConnectionStatusChangeCallback(), null);
            client.open();

            client.startTwin(new DeviceTwinStatusCallBack(), null, new OnProperty(), null);
            Map<Property, Pair<TwinPropertyCallBack, Object>> onDesiredPropertyChange = new HashMap<Property, Pair<TwinPropertyCallBack, Object>>() {
                {
                    put(new Property(App.TEMP_THRESHOLD, null),
                            new Pair<TwinPropertyCallBack, Object>(new OnProperty(), null));
                }
            };
            client.subscribeToTwinDesiredProperties(onDesiredPropertyChange);
            client.getTwin();

            client.subscribeToMethod(new DeviceMethodCallback() {
                @Override
                public DeviceMethodData call(String methodName, Object methodData, Object context) {
                    return handleDirectMethod(methodName, methodData, context);
                }
            }, null, 
            new IotHubEventCallback() {
                @Override
                public void execute(IotHubStatusCode responseStatus, Object callbackContext) {
                    System.out
                            .println("Direct method # IoT Hub responded to device method acknowledgement with status: "
                                    + responseStatus.name());
                }
            },
             null);

        } catch (Exception e) {
            e.printStackTrace();
            System.exit(1);
        }
    }





        public static DeviceMethodData handleDirectMethod(String methodName, Object methodData, Object context) {
            DeviceMethodData deviceMethodData;
            String payload = new String((byte[]) methodData);
            switch (methodName) {
            case "refreshData": {
                try {
                    int status = METHOD_SUCCESS;
                    List<HashMap<String, String>> res = ma.getDataChanged(payload);
                    if (context instanceof ModuleClient) {
                        ModuleClient client = (ModuleClient) context;
        
                        for (int i = 0; i < res.size(); i++) {
        
                            JSONObject json = new JSONObject(res.get(i));
                            System.out.println(json.toString());
                            Message message = new Message(json.toString().getBytes());
                            client.sendEventAsync(message, eventCallback, null, App.OUTPUT_NAME);
                        }
                    }
                    deviceMethodData = new DeviceMethodData(status, "Executed direct method " + methodName);
                } catch (NumberFormatException e) {
                    int status = INVALID_PARAMETER;
                    deviceMethodData = new DeviceMethodData(status, "Invalid parameter " + payload);
                }
                break;
            }
            case "SetModuleProperty": {
                System.out.println("Direct method - Set Module Property called with: " + payload);
                deviceMethodData = new DeviceMethodData(METHOD_SUCCESS, "Executed direct method " + methodName);
                break;
            }
            default: {
                int status = METHOD_NOT_DEFINED;
                deviceMethodData = new DeviceMethodData(status, "Not defined direct method " + methodName);
            }
            }
            return deviceMethodData;
        }
    

    protected static class DeviceTwinStatusCallBack implements IotHubEventCallback {
        @Override
        public void execute(IotHubStatusCode status, Object context) {
            System.out.println("IoT Hub responded to device twin operation with status " + status.name());
        }
    }

    // Device Twin callback to handle property updates， no use in this example
    protected static class OnProperty implements TwinPropertyCallBack {
        @Override
        public void TwinPropertyCallBack(Property property, Object context) {
            if (!property.getIsReported()) {
                if (property.getKey().equals(App.TEMP_THRESHOLD)) {
                    try {
                        long threshold = Math.round((double) property.getValue());
                        App.tempThreshold.set(threshold);
                    } catch (Exception e) {
                        System.out.println("Faile to set TemperatureThread with exception");
                        e.printStackTrace();
                    }
                }
            }
        }
    }


}
