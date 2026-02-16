/*!
 * ConvoScript - a chatbot library for Industrial Design education, using Data Foundry Local AI functionalities.
 * 
 * Copyright (c) 2024-2026 Jort Wiersma and Mathias Funk
 * 
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 *     https://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
class ConvoScript {
  // store all script arrays
  constructor({
    api_token,
    server,
    loadingElementSelector,
    resultElementSelector,
    inputElementSelector,
    acceptButtonElementSelector,
    delayTime = 100,
    errorMessage = `I have encountered a problem, have to go.`
  }) {
    if (
      [
        resultElementSelector,
        api_token,
        loadingElementSelector,
        inputElementSelector,
        acceptButtonElementSelector,
      ].some((param) => param === undefined || param === null)
    ) {
      throw new Error(
        `Not all required parameters provided: api_token: ${api_token}, resultElementSelector: ${resultElementSelector}, loadingElementSelector: ${loadingElementSelector}, inputElementSelector: ${inputElementSelector}, acceptButtonElementSelector: ${acceptButtonElementSelector}`
      );
    }

    // this.arrays = {};
    this.api_token = api_token;
    this.server = server;
    this.loadingElementSelector = loadingElementSelector;
    this.resultElementSelector = resultElementSelector;
    this.inputElementSelector = inputElementSelector;
    this.acceptButtonElementSelector = acceptButtonElementSelector;
    this.delayTime = delayTime;
    this.errorMessage = errorMessage;

    this.internalContext = {};
  }

  async run(array) {
    let manager = this;
    let latestImage, latestSound;
    let responses = [];
    this.internalContext.responses = responses;

    const hashtable = {
      textToText: foundry.textToText,
      textToImage: foundry.textToImage,
      textToSpeech: foundry.textToSpeech,
      textToRobot: foundry.textToRobot,
      imageToText: foundry.imageToText,
      soundToText: foundry.soundToText,
      //transcribeRecording: foundry.transcribeRecording,
      //transcribeFile: foundry.transcribeFile,
      stopRec: foundry.stopRec,
      popup: foundry.popup,
      models: foundry.models,
    };

    for (let i = 0; i < array.length; i++) {
      let step = array[i];
      if (step && step.role && step.role.toLowerCase() === "function") {

        if(step && typeof(step.content) === "function") {
          responses.push(await step.content())
          continue;
        }

        // resolve all function calls in step
        // either messages...
        if(step && step.messages) {
          for(const me of step.messages) {
            if(me && typeof(me) === 'object') {
              me.content = await processFunctionValue(me.content, responses);
            } else {
              let temp = await processFunctionValue(me, responses)
              if(typeof(temp) === 'string') {
                step.messages[index] = { role: 'user', content: temp }
              } else {
                step.messages[index] = temp;
              }
            }
          }
        }
        // ...or prompt
        else if(step && step.prompt) {
          step.prompt = await processFunctionValue(step.prompt, responses)
        }

        if (step && step.content === "fileSelector" && step.fileType) {
          let response = await hashtable[step.content](step.fileType);

          // Not ideal solution. step = { ...step, response: response} did work but was not usable in other functions somehow
          if (
            step.fileType.toLowerCase() === "sound" ||
            step.fileType.toLowerCase() === "audio"
          ) {
            latestSound = response;
            responses.push('');
          } else if (step.fileType.toLowerCase() === "image") {
            latestImage = response;
            responses.push('');
          }
        } else {
          loadingIndicator(true);
          let response = await hashtable[step.content]({
            api_token: this.api_token,
            server: this.server,
            image: latestImage,
            file: latestSound,
            prompt: manager.context().get("lastMessage"),
            logging: false,
            ...step,
          });
          loadingIndicator(false);

          if(response === undefined) {
            responses.push(manager.errorMessage);
            manager.context().set("lastMessage", manager.errorMessage)
            appendResult('assistant', manager.errorMessage);
            return;
          }

          step = { ...step, response: response };
          if (step.content === "textToImage") {
            latestImage = response;
            responses.push('');
            appendResult('assistant', `<img src="${response}" />`);
          } else if (step.content === "textToSpeech") {
            latestSound = response;
            responses.push('');
            appendResult('assistant', `<audio controls src="${latestSound}" />`);
          } else if (step.content === "textToRobot") {
            latestSound = response;
            responses.push('');
            appendResult('assistant', `<audio controls src="${latestSound}" />`);
          } else if (step.content === "fileSelector") {
            responses.push('');
            appendResult('user', `File Selected`);
          } else {
            loadingIndicator(true);
            await delay(response.length * 10);
            loadingIndicator(false);
            manager.context().set("lastMessage", response)
            responses.push(response);
            if(step.output !== false) {
              appendResult('assistant', response);
            }
          }
        }
      } else if (step.input) {
        let msg = await this.waitForUserInput(step.input);
        if (
          Array.isArray(step.input) ||
          step.input.toLowerCase() === "text" ||
          step.input.toLowerCase() === "transcription"
        ) {
          manager.context().set("lastMessage", msg);
          manager.context().set("lastUserInput", msg);
          appendResult(step.role, msg);
          step = { ...step, content: msg };
        }
        else if (step.input.toLowerCase() === "image") {
          latestImage = msg;
          appendResult(step.role, `<img src="${msg}" />`);
        }
        else if (
          step.input.toLowerCase() === "sound" ||
          step.input.toLowerCase() === "audio"
        ) {
          latestSound = msg;
          appendResult(step.role, `<audio controls src="${msg}" />`);
        }
        step = { ...step, content: msg };
        responses.push(msg);
      } else {
        // preprocess content of step
        let content = await processFunctionValue(step.content, responses);

        await delay(this.delayTime + Math.random() * 1000);
        loadingIndicator(true);
        await delay(content.length * 80);
        loadingIndicator(false);
        responses.push(content);
        appendResult(step.role, content);
        manager.context().set("lastMessage", content)
      }

      if(step.context) {
        let temp = manager.context().get("lastMessage");
        manager.context().set(step.context, temp);
      }

      window.scrollTo(0, document.body.scrollHeight);
      await delay(this.delayTime);
    }

    async function processFunctionValue(value, responses) {
      if(value === undefined) {
        value = ''
      } else {
        if(typeof(value) === 'function') {
          value = await value(manager.context());
        }
      }
      return value;
    }

    function delay(ms) {
      return manager.delay(ms);
    }
    function appendResult(role, message) {
      let res;
      if(role === 'assistant') {
        res = `<div class="msg-left">
          <p class="role">${manager.capitalize(role)}</p>
          <article>${message}</article>
        </div>`
      } else {
        res = `<div class="msg-right">
          <p class="role">${manager.capitalize(role)}</p>
          <article>${message}</article>
        </div>`
      }

      document.querySelector(manager.resultElementSelector).insertAdjacentHTML("beforeend", res);
    }

    function loadingIndicator(show) {
      if(show) {
        document.querySelector(manager.loadingElementSelector).removeAttribute("hidden");
      } else {
        document.querySelector(manager.loadingElementSelector).setAttribute("hidden", "");
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  context() {
    return {
      steps: (index) => (index < 0 
        ? this.internalContext.responses[this.internalContext.responses.length + index] 
        : this.internalContext.responses[index]),
      lastMessage: () => this.context().get("lastMessage"),
      lastUserInput: () => this.context().get("lastUserInput"),
      get: (key, defaultValue = "") => (this.internalContext[key] ? this.internalContext[key] : defaultValue),
      set: (key, value) => {
        console.log(`Convo context: ${key} -> ${value}`);
        if(value) {
          this.internalContext[key] = value
        } else {
          delete this.internalContext[key]
        }
      },
      waitFor: async (key, timeout = -1) => {
        let timeElapsed = 0
        console.log(`Convo wait for ${key}...`);
        while(!this.internalContext[key]) {
          await this.delay(100)
          if(timeout > -1) {
            timeElapsed += 100
            if(timeout < timeElapsed) {
              break;
            }
          }
        }
        console.log(`Convo wait for ${key} done: ${this.internalContext[key]}`);
        return this.internalContext[key];
      }
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

  delay(ms) {
    return new Promise((resolve) => setTimeout(resolve, Math.min(ms, 2500)));
  }

  waitForUserInput(type) {
    return new Promise((resolve) => {
      // add multiple option buttons
      if(Array.isArray(type)) {
        document.querySelector(this.resultElementSelector).insertAdjacentHTML("beforeend", `<div id="optionsBubble" class="msg-right">
          <p class="role">User</p>
          <article id="optionsContainer"></article>
        </div>`);
        let options = document.querySelector('#optionsContainer');
        type.forEach(str => {
          // create a button
          const btn = document.createElement('button');
          btn.style.backgroundColor = 'var(--convo-msg-left)';
          btn.style.color = 'var(--pico-primary)';
          btn.textContent = str;
          btn.dataset.option = str;
          btn.addEventListener('click', () => {
            resolve(btn.dataset.option);
            document.querySelector('#optionsBubble').remove();
          });
          options.appendChild(btn);
        });
      } 
      // text input
      else if (type && type.toLowerCase() === "text") {
        document
          .querySelector(this.inputElementSelector)
          .removeAttribute("hidden"); // show input
        document
          .querySelector(this.acceptButtonElementSelector)
          .removeAttribute("hidden"); // show button
        document
          .querySelector(this.inputElementSelector).focus();

        let fct = () => {
            resolve(document.querySelector(this.inputElementSelector).value); // resolve
            document
              .querySelector(this.acceptButtonElementSelector)
              .setAttribute("hidden", ""); // remove button
            document
              .querySelector(this.inputElementSelector)
              .setAttribute("hidden", ""); // remove input
            document.querySelector(this.inputElementSelector).value = ""; // clear input
          }
        document.querySelector(this.inputElementSelector).onkeyup =
          (event) => {
            if(event.key === "Enter" && document.querySelector(this.inputElementSelector).value.length > 0) { fct(); }
          }
        document.querySelector(this.acceptButtonElementSelector).onclick = fct;
      } 
      // media input
      else if (
        type && type.toLowerCase() === "image" ||
        type && type.toLowerCase() === "audio" ||
        type && type.toLowerCase() === "sound"
      ) {
        document
          .querySelector(this.acceptButtonElementSelector)
          .removeAttribute("hidden");
        document.querySelector(this.acceptButtonElementSelector).innerHTML =
          "Upload";
        document.querySelector(this.acceptButtonElementSelector).onclick =
          async () => {
            let file = await foundry.fileSelector(type);
            if (type && type.toLowerCase() !== "image") {
              try {
                file = this.processAudioFile(file);
              } catch (err) {
                console.error(err);
              }
            }

            resolve(file);
            document
              .querySelector(this.acceptButtonElementSelector)
              .setAttribute("hidden", "");
          };
      } else if (type && type.toLowerCase() === "transcription") {
        this.initializeMicrophone();
        document
          .querySelector(this.acceptButtonElementSelector)
          .removeAttribute("hidden");
        document.querySelector(this.acceptButtonElementSelector).innerHTML =
          "Hold to record";
        document.querySelector(this.acceptButtonElementSelector).ontouchstart =
          async () => {
            foundry.transcribeRecording({ api_token });
          };
        document.querySelector(this.acceptButtonElementSelector).onmousedown =
          async () => {
            foundry.transcribeRecording({ api_token });
          };
        document.querySelector(this.acceptButtonElementSelector).ontouchend =
          async () => {
            let transcription = await foundry.stopRec({ api_token });
            resolve(transcription);
            document
              .querySelector(this.acceptButtonElementSelector)
              .setAttribute("hidden", "");
          };
        document.querySelector(this.acceptButtonElementSelector).onmouseup =
          async () => {
            let transcription = await foundry.stopRec({ api_token });
            resolve(transcription);
            document
              .querySelector(this.acceptButtonElementSelector)
              .setAttribute("hidden", "");
          };
      }
    });
  }

  // Capitalize the first letter of input
  capitalize(input) {
    return input.charAt(0).toUpperCase() + input.slice(1);
  }

  // Add RecordRTC and ask for microphone access at the right moment
  initializeMicrophone(logging = false) {
    if (typeof RecordRTC !== "undefined") {
      if (logging) {
        console.log("RecordRTC is already loaded.");
      }
    } else {
      if (logging) {
        console.log("RecordRTC is not loaded, adding script to the page.");
      }
      // Create a script element to load RecordRTC from CDN
      var script = document.createElement("script");
      script.src = "https://cdn.webrtc-experiment.com/RecordRTC.js";
      script.async = true;

      // Set up callback when the script is loaded
      script.onload = function () {
        if (logging) {
          console.log("RecordRTC has been loaded.");
        }
      };

      // Append script to the head of the document
      document.head.appendChild(script);
    }
    navigator.mediaDevices.getUserMedia({
      audio: true,
    });
  }

  // Function to process incoming or recorded audio file to be able to be played by the user
  processAudioFile(file) {
    return new Promise((resolve, reject) => {
      const reader = new FileReader();
      reader.onload = function (e) {
        resolve(e.target.result);
      };
      reader.onerror = function (e) {
        reject("Error reading file");
      };
      reader.readAsDataURL(file);
    });
  }
}
