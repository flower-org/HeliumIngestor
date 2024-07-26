# How to create a CommandCamera for USB/webcam in Linux:

Please note that in Windows/MacOs command is formed in a different way, please refer to [URL HERE].


## 1. Find your device descriptors in the system:

Find video devices (typically in /dev/video*):
`ffmpeg -f v4l2 -list_formats all -i /dev/video0`

Find audio devices:
`alsamixer` - to set mic volume
`apt install alsa-utils`
`arecord -L`


## 2. Create command prefix for the config:

- Example commands:
  - Video and Audio:
`ffmpeg -f alsa -i plughw:CARD=camera,DEV=0 -f v4l2 -i /dev/video0 -c:v libx264 -c:a aac -b:a 192k -preset fast -segment_format mp4`

  - Video only:
`ffmpeg -f v4l2 -i /dev/video0 -c:v libx264 -preset fast -segment_format mp4`

**Note**: create command prefix and test the full command given that the following suffix will be added:
```java
static final String FFMPEG_GENERIC_CMD_PATTERN =
                " -timeout %d -f segment -segment_time 00:00:01 -strftime 1" +
                " -reset_timestamps 1 %s/%s/video_%%Y-%%m-%%d_%%H_%%M_%%S.mp4";
```

To clarify:  
- If you test the command:  
`ffmpeg -f v4l2 -i /dev/video0 -c:v libx264 -preset fast -segment_format mp4 -timeout 1000000 -f segment -segment_time 00:00:01 -strftime 1 -reset_timestamps 1 /home/john/cam/usbcam/video_%Y-%m-%d_%H_%M_%S.mp4`

- Then in config, specify the prefix:
`ffmpeg -f v4l2 -i /dev/video0 -c:v libx264 -preset fast -segment_format mp4`

## 3. RawCommandCamera

You can use this way of configuring your video input, and it just takes your command as is and runs it, i.e. it doesn't add suffixes or alter the command in any way.  
The downside is that you'll have to specify timeouts and output folders manually and keep them in sync manually.  
Also make sure it produces video segments properly.  
Inconvenient, use only if you really need to do something hacky.

## 4. Complications

- Make sure that your command produces multiple segments properly when testing.


- Most of cheap cameras provide "raw video" - mjpeg, yuyv422, etc.
  To save on space, this video needs to be compressed on the fly - which is CPU intensive (in the example command `-c:v libx264 -c:a aac`).
  Ideally you want to purchase a camera that's capable of outputting compressed video (like h.264), which we can save as is - doesn't need to be re-encoded for space efficiency.
  In this case the compression part should be removed from the command.  
  To summarize, what you save on cheap cameras will have to be carried by your CPU, and this approach doesn't scale.


- Some cheap cameras have issues providing audio and video at the same time for some reason. I'm not researching ffmpeg in depth or troubleshooting cheap cameras hw (DIY).


- Java Process API has huge problems running commands with special symbols, and I don't prioritize debugging this one either.  
More specifically, if you use `Runtime.exec(...)` to run a command like the following, it will be malformed:  
`ffmpeg -f alsa -i plughw:CARD=camera,DEV=0 -f v4l2 -i /dev/video0 -c:v libx264 -c:a aac -b:a 192k -preset fast -force_key_frames "expr:gte(t,n_forced*1)" -f segment -segment_time 00:00:01 -reset_timestamps 1 -strftime 1 -segment_format mp4 video_%Y-%m-%d_%H_%M_%S.mp4`
because of the keyframe forcing part, that has symbols like `"`
`"expr:gte(t,n_forced*1)"`  
I'm not sure what the problem is, I tried some random symbol escaping and it didn't work.  
The command examples from (2) don't use such symbols and work fine for me.  
Please be aware of this issue, because it's super unobvious and frustrating.

